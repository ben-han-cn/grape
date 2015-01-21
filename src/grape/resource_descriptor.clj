(ns grape.resource-descriptor
  (:require [grape.exception :as ex]
            [grape.util :as util]
            [grape.resource-tree :as tt]
            [grape.resource-relationship :as tr]
            [clojure.set :as s])
  (:use [clojure.string :only [join]]))

(defrecord ResourceDescriptor [resource-tree resource-relationship resource-fields])

(def ^:dynamic *descriptor* nil)

(defn mk-resource-descriptor []
  (->ResourceDescriptor (tt/mk-resource-tree [:root])
                        (-> (tr/mk-resource-relationship)
                            (tr/declare-relationship :owned-by :own)
                            (tr/declare-relationship :has-many :used-by))
                        {}))

(defn type->table-name [t] 
  (keyword (str "zdns_" (-> t name (.replaceAll "-" "_")))))

(defn usage->table-name [& types]
  (keyword 
      (clojure.string/join "$" 
            (map (comp name type->table-name) types))))

(defn field-and-types [t]
  (get-in *descriptor* [:resource-fields t :fields]))

(defn fields [t]
  (keys (field-and-types t)))

(defn owners [t]
  (tr/get-relationship (:resource-relationship *descriptor*)
                       t
                       :owned-by))

(defn uses [t]
  (tr/get-relationship (:resource-relationship *descriptor*)
                       t
                       :has-many))

(defn resource-types []
  (tt/types (:resource-tree *descriptor*)))

(defn all-usages []
  (let [ts (resource-types)]
    (reduce (fn [usages t]
              (let [usage (uses t)]
                (if (empty? usage)
                  usages
                  (merge usages {t usage})))) {} ts)))

(defn id-fields [t]
  (get-in *descriptor* [:resource-fields t :id-fields]))

(defn type-of-field [t field]
  (get-in *descriptor* [:resource-fields t :fields field]))

(defn create-table-params-for-type [t]
  (let [owners' (owners t)]
    [(type->table-name t)
     (field-and-types t)
     {:primary-keys [:id]
      :unique-keys (id-fields t)
      :references 
     (reduce (fn [ownership owner]
               (assoc ownership owner [(type->table-name owner) :owned-by :id]))
             {} owners')}]))

(defn create-table-sql-for-usage [user resource]
  [(usage->table-name user resource)
   (reduce (fn [usage-ids t]
             (assoc usage-ids t (type-of-field t :id)))
           {} [user resource])
   {:unique-keys [user resource]
    :references {user [(type->table-name user) :owned-by :id]
                 resource [(type->table-name resource) :reference :id]}}])

(defn resource->row [t resource]
  ((get-in *descriptor* [:resource-fields t :resource->row]) resource))

(defn generate-id [t resource]
  ((get-in *descriptor* [:resource-fields t :generate-id]) resource))

(defn row->resource [t row]
  ((get-in *descriptor* [:resource-fields t :row->resource]) row))

; since column of rational db doesn't support array
; we need to transform string array into string
; encode method: 
; str-len(3 character) + string + ;str-len + string +...
(defn- string-ci->db-val [string]
  (clojure.string/lower-case string))

(defn- domain->db-val [domain]
  (util/fqdn domain))

; transfer fild value into db field
(def ^:const field-adapters
  {:string [identity identity]
   :integer [identity identity]
   :string-array [identity identity]
   :integer-array [identity identity]
   :domain [domain->db-val identity]
   :string-ci [string-ci->db-val identity]
   :text [identity identity]})

(defn field->column [t field]
  ((-> field-adapters t first) field))

(defn column->field [t col]
  ((-> field-adapters t second) col))

(def uuid (fn[] (str (java.util.UUID/randomUUID))))

(defn- add-db-transformer [descriptor t attrs]
  (let [id-fields (set (:id-fields attrs))
        fields (binding [*descriptor* descriptor]
                 (reduce (fn [all-fields owner]
                           (merge all-fields {owner (type-of-field owner :id)}))
                         (:fields attrs) (owners t)))
        iteral-id-field {:id (if (= 1 (count id-fields))
                               ((first id-fields) fields)
                               :string)}
        transform-fields (fn [resource]
                           (reduce (fn [row [field-name field-type]]
                                     (if-let [value (field-name resource)]
                                       (assoc row field-name (field->column field-type value))
                                       row))
                                   resource fields))
        generate-id (fn [resource]
                      (if (nil? (:id resource))
                        (if (= (count id-fields) 1)
                          (assoc resource :id (get resource (first id-fields)))
                          (assoc resource :id (uuid)))
                        resource))
        row->resource (fn [row]
                        (reduce (fn [resource [field-name field-type]]
                                  (update-in resource [field-name] #(column->field field-type %)))
                                row fields))]
    (-> attrs
        (merge {:resource->row transform-fields
                :generate-id generate-id
                :row->resource row->resource})
        (assoc :fields (merge fields iteral-id-field)))))

(defn- add-relationships! [relationship tree from r-type tos]
  (dorun (map #(tr/add-relationship! relationship tree from r-type %) tos)))

(defn- check-attr-validation [descriptor t attrs]
  (let [fields (-> (:fields attrs) keys set)
        id-fields (-> (:id-fields attrs) set)
        field-types (-> (:fields attrs) vals set)
        has-many (-> (:has-many attrs) set)
        owned-by (-> (:owned-by attrs) set)]
    (when (contains? fields :id)
      (ex/throw (str (name t) " specify id field which should be generated by grape") :resource-descriptor))
    (when-not (s/subset? field-types (-> field-adapters keys set))
      (ex/throw (str (name t) " field type isn't supported") :resource-descriptor))
    (when-not (empty? (s/intersection fields owned-by (set (map #(util/keyword-append % "s") has-many))))
      (ex/throw (str (name t) " fields, owned-by and has-many name conflict") :resource-descriptor))
    (when-not (empty? (s/intersection owned-by has-many))
      (ex/throw (str (name t) " owned-by has-many has intersection") :resource-descriptor))
    (when-not (s/subset? id-fields (s/union fields owned-by))
      (ex/throw (str (name t) " id field isn't subset of fields") :resource-descriptor))))

(defn declare-resource [descriptor t attrs]
  (let [{:keys [resource-tree resource-relationship resource-fields]} descriptor]
    (check-attr-validation descriptor t attrs)
    (add-relationships! resource-relationship resource-tree t :owned-by (:owned-by attrs))
    (add-relationships! resource-relationship resource-tree t :has-many (:has-many attrs))
    (update-in descriptor [:resource-fields] #(assoc % t (add-db-transformer descriptor t attrs)))))

(defn- import-parent-and-child-to-owned-by! [resource-relationship resource-tree hierarchy]
  (letfn [(parent-child->owned-by [type-chain]
            (dorun (map (fn [[parent child]]
                          (tr/add-relationship! resource-relationship resource-tree child :owned-by parent))
                        (partition 2 1 type-chain))))]
    (dorun (map parent-child->owned-by hierarchy))))

(defn declare-type-hierarchy [descriptor & hierarchy]
  (let [{:keys [resource-tree resource-relationship]} descriptor
        new-resource-tree (tt/declare-type-hierarchy resource-tree hierarchy)]
    (import-parent-and-child-to-owned-by! resource-relationship new-resource-tree hierarchy)
    (assoc descriptor :resource-tree new-resource-tree)))
