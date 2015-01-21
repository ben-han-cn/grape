(ns grape.jdbc-wrapper
  (:require [clojure.java.jdbc :as jdbc]
            [grape.util :as util]
            [grape.exception :as ex])
  (:use [clojure.string :only [join]]))

; add array support, this solution is referred to
; http://stackoverflow.com/questions/22959804/inserting-postgresql-arrays-with-clojure
(extend-protocol jdbc/ISQLParameter
    clojure.lang.IPersistentVector
    (set-parameter [v ^java.sql.PreparedStatement stmt ^long i]
          (let [conn (.getConnection stmt)
                          meta (.getParameterMetaData stmt)
                          type-name (.getParameterTypeName meta i)]
                  (if-let [elem-type (when (= (first type-name) \_) (apply str (rest type-name)))]
                            (.setObject stmt i (.createArrayOf conn elem-type (to-array v)))
                            (.setObject stmt i v)))))

(extend-protocol jdbc/ISQLParameter
    clojure.lang.Seqable
    (set-parameter [seqable ^java.sql.PreparedStatement s ^long i]
          (jdbc/set-parameter (vec (seq seqable)) s i)))

(extend-protocol jdbc/IResultSetReadColumn
  java.sql.Array
  (result-set-read-column [val _ _]
    (into [] (.getArray val))))

; drop all the create tables
(defn- create-drop-function [db]
  (let [command "CREATE OR REPLACE FUNCTION drop_tables() RETURNS void AS $$  
                DECLARE
                statements CURSOR FOR
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public';
                BEGIN
                FOR stmt IN statements LOOP
                EXECUTE 'DROP TABLE '|| quote_ident(stmt.tablename) || ' CASCADE;'; 
                END LOOP;
                END;
                $$ LANGUAGE plpgsql;"]
    (jdbc/db-do-commands db command)))

(defmacro with-transaction [& body]
  `(with-open [^java.sql.Connection con# (jdbc/get-connection db)]
     (binding [db (jdbc/add-connection db con#)]
       (jdbc/db-transaction* db (^{:once true} fn* [_db#] ~@body)))))

(defmacro without-transaction [action]
  `(~@action :transaction? false))

(defn init-db [db-url]
  (create-drop-function db-url)
  (def ^:dynamic db db-url))

(defn del-db []
  (without-transaction (jdbc/query db "select drop_tables();")))

(defn- do-query [query]
  (without-transaction (jdbc/query db query)))

(defn- do-insert [table-name row]
  (without-transaction (jdbc/insert! db table-name row)))

(defn- do-delete [table-name row]
  (without-transaction (jdbc/delete! db table-name row)))

(defn- do-update [table-name new-data old-data]
  (without-transaction (jdbc/update! db table-name new-data old-data)))

(def ^:const type-map {:string "VARCHAR(64)"
                       :string-ci "VARCHAR(64)"
                       :domain "VARCHAR(255)"
                       :integer :integer
                       :real :real
                       :serial :serial
                       :time :timestamp
                       :text :text
                       :integer-array "integer[]" 
                       :string-array "text[]"})

(defn key->col [key]
  (-> key name (.replaceAll "-" "_") keyword))

(defn col->key [key]
  (-> key name (.replaceAll "_" "-") keyword))

(defn- record->row [record]
  (util/change-map-key record #(key->col %)))

(defn- row->record [row]
  (util/change-map-key row #(col->key %)))

; generate sql like 
;   (age integer,teacher_name VARCHAR(64),name VARCHAR(64),PRIMARY KEY (name,age))
(defn- description-for-col [cols {:keys [primary-keys unique-keys references]}]
  (let [key->col-str (comp name key->col)
        col-name-and-types (map (fn [[col-name col-type]]
                                  (str (key->col-str col-name) 
                                       " "
                                       (name (col-type type-map))
                                       (if (contains? references col-name)
                                        (let [[table r-type pk] (col-name references)]
                                             (str " NOT NULL REFERENCES "
                                                  (name table)
                                                  "(" (key->col-str pk) ")"
                                                  " ON DELETE "
                                                  (if (= :owned-by r-type)
                                                    "CASCADE"
                                                    "RESTRICT")))
                                        "")))
                                  cols)
        col-spec (join "," col-name-and-types)
        primary-key-spec  (if (empty? primary-keys)
                            ""
                            (str ", PRIMARY KEY (" 
                                 (join "," (map key->col-str primary-keys)) 
                                 ")"))
        unique-key-spec (if (empty? unique-keys)
                            ""
                            (str ", UNIQUE(" 
                                 (join "," (map key->col-str unique-keys)) 
                                 ")"))]
    (str "(" col-spec primary-key-spec unique-key-spec")")))

(defn- record->sql [record]
  (let [[ks vs] (reduce-kv (fn [[kc vc] k v]  
                             [(concat kc [(str (name (key->col k)) "=?")]) 
                              (concat vc [v])]) [[] []] record)]
    (into [(join " and " ks)] vs)))

(defn get-rows [table-name attrs]
  (let [[where & values] (record->sql attrs)
        query (cons (str "SELECT * FROM " (name table-name) " WHERE " where) values)
        rows (do-query query)]
    (if (empty? rows) nil (map row->record rows))))

(defn get-ids 
  ([table-name]
   (let [query (str "SELECT id FROM " (name table-name))
         rows (do-query query)]
     (if (empty? rows) nil (map :id rows))))
  ([table-name attrs]
   (let [[where & values] (record->sql attrs)
         query (cons (str "SELECT id FROM " (name table-name) " WHERE " where) values)
         rows (without-transaction (jdbc/query db query))]
     (if (empty? rows) nil (map :id rows)))))

(defn get-all-rows [table-name]
  (let [query (str "SELECT * FROM " (name table-name))
        rows (do-query query)]
    (if (empty? rows) nil (map row->record rows))))

(defn get-row [table-name attrs]
  (let [rows (get-rows table-name attrs)]
    (if (nil? rows) rows (first rows))))

(defn get-max-id [table-name]
  (let [query (str "SELECT MAX(id) FROM " (name table-name))]
    (-> (do-query query)
        first
        vals
        first)))

(defn create-table [table-name cols params]
   (let [params' (merge {:primary-keys []
                         :unque-keys []
                         :reference {}} params)
         command (str "CREATE TABLE " (name table-name) (description-for-col cols params'))]
     (jdbc/db-do-commands db command)
     table-name))

(defn del-table [table-name]
  (jdbc/db-do-commands db (jdbc/drop-table-ddl table-name)))

(defn table-exists? [table-name]
  (try 
    (jdbc/query db [(str "SELECT NULL FROM " (name table-name) " LIMIT 1")])
    true
  (catch java.sql.SQLException _ false)))

(defn has-table? [table-name]
  (table-exists? table-name))

(defn get-all-table-names []
  (map :tablename 
       (with-transaction 
         (jdbc/query db ["SELECT  tablename  FROM  pg_tables  
                         WHERE  tablename  NOT  LIKE  'pg%'  
                         AND tablename NOT LIKE 'sql_%' 
                         ORDER  BY  tablename"]))))

(defn insert-row [table-name record]
  (do-insert table-name (record->row record)))

(defn del-row [table-name record]
  (do-delete table-name (record->sql record)))

(defn update-row [table-name old-data new-data]
  (do-update table-name (record->row new-data) (record->sql old-data)))
