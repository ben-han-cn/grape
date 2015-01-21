(ns grape.exception)

(defn throw [message module]
  (throw (ex-info message {:module module})))

(defn throw' [message data]
  (throw (ex-info message data)))

(defmacro catch [handler & action]
  `(try
     ~@action
    (catch clojure.lang.ExceptionInfo e#
     (let [msg# (.getMessage e#)
           mod# (-> e# ex-data :module)]
       (~handler msg# mod#)))))

(defn stack-trace-to-str[^java.lang.Exception e depth]
  (let [stack (.getStackTrace e)
        depth (min depth (dec (count stack)))]
    (->> (take depth stack)
      (map str)
      (clojure.string/join "\n\t"))))

(defmacro catch' [handler & action]
  `(try
     ~@action
    (catch clojure.lang.ExceptionInfo e#
     (let [msg# (.getMessage e#)
           data# (ex-data e#)]
       (~handler msg# data#)))
    (catch java.lang.Exception e1#
      (let [call-stack# (stack-trace-to-str e1# 20)
            msg1# (.getMessage e1#)
            msg1# (if (empty? msg1#) (class e1#) msg1#)]
        (~handler (str msg1# "\n\tcall stack:\n\t" call-stack#) nil)))))
