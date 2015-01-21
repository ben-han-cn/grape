(ns grape.resource-store-test
  (:require [clojure.test :refer :all]
            [grape.resource-descriptor :as descriptor] 
            [grape.resource-store :as store] 
            [grape.resource-usage :as usage] 
            [grape.jdbc-wrapper :as jw]
            [grape.exception :as ex] 
            [grape.util :as util]))

(def db-url {:subprotocol "postgresql"
             :subname "zcloud"
             :user "zcloud"
             :password "zcloud"})

(def resource-descriptor
  (-> (descriptor/mk-resource-descriptor)
      (descriptor/declare-type-hierarchy [:view] [:zone :rrset] [:zcuser] [:acl] [:rrsetweight])
      (descriptor/declare-resource :acl {:fields {:name :string :ips :string-array} :id-fields [:name]})
      (descriptor/declare-resource :view {:fields {:name :string} :id-fields [:name] :has-many [:acl]})
      (descriptor/declare-resource :zcuser {:fields {:name :string} :id-fields [:name]})
      (descriptor/declare-resource :zone {:fields {:name :domain } :id-fields [:name] :owned-by [:zcuser]})
      (descriptor/declare-resource :rrset {:fields {:name :domain
                                                    :type :string-ci 
                                                    :ttl :integer 
                                                    :rdatas :string-array}
                                           :owned-by [:view]
                                           :id-fields [:name :type :view]})
      (descriptor/declare-resource :rrsetweight {:fields {:weights :integer-array} :owned-by [:rrset]})))

(defn- db-setup-and-clean [f]
  (jw/init-db db-url)
  (store/init-resource-store resource-descriptor)
  (f)
  (jw/del-db))

(use-fixtures :each db-setup-and-clean)


(deftest resource-manage-test
  (testing "add/get/delete resource"
    (store/add-resource :acl {:name "local" :ips ["1.1.1.1" "2.2.2.2"]})
    (store/add-resource :acl {:name "default" :ips ["3.3.3.3"]})
    (store/add-resource :view {:name "v1" :acls ["local"]})
    (store/add-resource :zcuser {:name "liyuan"})
    (store/add-resource :zone {:name "cn" :zcuser "liyuan"})
    (store/add-resource :zone {:name "com" :zcuser "liyuan"})
    (store/add-resource :rrset {:name "knet.cn"
                                :type "a"
                                :ttl 3600
                                :rdatas ["1.1.1.1" "3.3.3.3"]
                                :view "v1"
                                :zone "cn"})
    (store/add-resource :zcuser {:name "ben"})
    (is (= 1 (count (store/get-resources :acl {:ips ["1.1.1.1" "2.2.2.2"]}))))
    (let [acl (first (store/get-resources :acl {:name "local"}))]
      (is (= (:ips acl) ["1.1.1.1" "2.2.2.2"])))
    (is (= 1 (count (store/get-resources :view))))
    (is (= {:name "v1" :acls ["local"]} (dissoc (first (store/get-resources :view {:name "v1"})) :id)))
    (is (= 0 (count (store/get-resources :view {:name "v2"}))))
    (is (= 2 (count (store/get-resources :zone))))
    (is (= 1 (count (store/get-resources :zone {:name "Cn"}))))
    (is (= {:zcuser "liyuan" :name  "com."} (dissoc (first (store/get-resources :zone {:name "com"})) :id)))
    (is (= 1 (count (store/get-resources :zone {:name "cn."}))))
    (is (= 1 (count (store/get-resources :rrset {:zone "cn" :name "knet.cn" :type "A" :view "v1"}))))
    (is (= {:zone "cn." :name "knet.cn." :type "a" :ttl 3600 :rdatas ["1.1.1.1" "3.3.3.3"] :view "v1"} 
           (dissoc (first (store/get-resources :rrset {:zone "Cn" :name "knet.cN" :type "A" :view "v1"})) :id)))
    (store/update-resource :rrset {:zone "cn" :name "knet.cn"} {:ttl 7200})
    (try
      (store/update-resource :rrset {:zone "cn" :name "knet.cn"} {:view "v4"})
      (catch Exception e ()))
    (is (= {:zone "cn." :name "knet.cn." :type "a" :ttl 7200 :rdatas ["1.1.1.1" "3.3.3.3"] :view "v1"} 
           (dissoc (first (store/get-resources :rrset {:zone "cn" :name "knet.cn" :type "a"})) :id)))

    (let [rrset-id (:id (first (store/get-resources :rrset {:zone "cn" :name "knet.cn" :type "a"})))]
      (store/add-resource :rrsetweight {:rrset rrset-id
                                        :weights[1 2 3]})
      (is (= rrset-id (:rrset (first (store/get-resources :rrsetweight))))))

    (store/delete-resource :zone {:name "cn"})
    (is (= 0 (count (store/get-resources :rrset {:name "knet.cn" 
                                                 :zone "cn"
                                                 :type "a"}))))
    (is (= 0 (count (store/get-resources :rrsetweight))))
    (is (= 1 (count (store/get-resources :zone)))) 
    (store/delete-resource :view {:name "v1"})
    (is (= 0 (count (store/get-resources :view))))
    (store/delete-resource :acl {:name "default"})
    (is (= 1 (count (store/get-resources :acl))))))

(deftest resource-usage-test
  (testing "resource usage management"
    (store/add-resource :acl {:name "local" :ips ["1.1.1.1" "2.2.2.2"]})
    (store/add-resource :acl {:name "default" :ips ["3.3.3.3"]})
    (store/add-resource :view {:name "v1" :acls ["local" "default"]})
    (let [view (first (store/get-resources :view {:name "v1"}))
          acls (:acls view)]
      (is (= 2 (count acls)))
      (is (= #{"default" "local"}(set acls))))
    (try
      (store/add-resource :view {:name "v2"
                                 :acls ["dianxin"]})
      (is (= 1 0))
      (catch Exception e ()))
    (let [view (first (store/get-resources :view {:name "v2"}))]
      (is (empty? (:acls view))))

    (try
      (store/delete-resource :acl {:name "local"})
      (is (= 1 0))
      (catch Exception e ()))
    (let [view (first (store/get-resources :view {:name "v1"}))
          acls (:acls view)]
      (is (= 2 (count acls))))

    (store/update-resource :view {:name "v1"} {:acls ["default"]})
    (store/delete-resource :acl {:name "local"})
    (let [acls (store/get-resources :acl)]
      (is (= 1 (count acls))))))

(deftest transaction-test
  (testing "transaction test"
    (ex/catch'
      (fn [msg _] ())
      (store/with-transaction
        (store/add-resource :zcuser {:name "b"})
        (store/add-resource :zcuser {:nade "k"})))
    (is (= 0 (count (store/get-resources :zcuser))))
    (store/with-transaction
      (store/add-resource :zcuser {:name "b"})
      (store/add-resource :zcuser {:name "k"}))
    (is (= 2 (count (store/get-resources :zcuser))))
    (ex/catch'
      (fn [msg _] ())
      (store/with-transaction
        (store/add-resource :view {:name "v2"
                                 :acls ["dianxin"]})))
    (is (empty? (store/get-resources :view)))))
