(ns grape.util)

(defn ^String chop [^String source ^String postfix]
  (if (.endsWith source postfix)
    (.substring source 0 (- (.length source)
                            (.length postfix)))
    source))

(defn change-map-key [m f]
  (reduce-kv (fn [ret k v] (assoc ret (f k) v)) 
             {} 
             m))

(defn split-map [m f]
  (reduce (fn [[t-m f-m] [k v]]
            (if (f k v)
              [(conj t-m {k v}) f-m]
              [t-m (conj f-m {k v})]))
          [{} {}] m))

(defn split-vec [v f]
  (reduce (fn [[t-v f-v] elem]
            (if (f elem)
              [(conj t-v elem) f-v]
              [t-v (conj f-v elem)]))
          [[] []] v))

(defn split-map-with-keys [m ks]
  (split-map m (fn [k v] ((set ks) k))))

(defn keyword-trip [k v]
  (keyword (chop (name k) (name v))))

(defn keyword-append [k v]
  (keyword (str (name k) (name v))))

(defn is-fqdn?[n]
    (.endsWith n ".")) 

(defn fqdn[n]
  (let [lcase-n (clojure.string/lower-case n)] 
    (if-not (is-fqdn? lcase-n)
      (str lcase-n ".")
      lcase-n)))
