(defproject grape "0.1.0"
  :description "a simple orm based on postgresql"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [postgresql "9.1-901.jdbc4"]]
  :source-paths ["src"]
  :test-paths ["test"])
