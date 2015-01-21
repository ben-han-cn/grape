(ns grape.resource-relationship-test
  (:require [clojure.test :refer :all]
            [grape.resource-tree :as type-tree] 
            [grape.resource-relationship :as type-rel] 
            [grape.exception :as ex]))

(def types (type-tree/declare-type-hierarchy (type-tree/mk-resource-tree [:root]) 
                                             [
                                            [:view :zone :rrset] 
                                            [:user] 
                                            [:acl] 
                                            [:rrweight]]))  

(def type-relationships 
  (-> (type-rel/mk-resource-relationship)
      (type-rel/declare-relationship :owned-by :own)
      (type-rel/declare-relationship :has-many :used-by)
      (type-rel/add-relationship! types :view :has-many :acl)
      (type-rel/add-relationship! types :zone :owned-by :user)
      (type-rel/add-relationship! types :rrset :owned-by :view)
      (type-rel/add-relationship! types :rrweight :owned-by :rrset)))

(deftest type-relationship-test
  (testing "test owned-by has-many relationship"
    (is (= #{:acl} (type-rel/get-relationship type-relationships :view :has-many))) 
    (is (= #{:rrset} (type-rel/get-relationship type-relationships :view :own))) 
    (is (= #{:view} (type-rel/get-relationship type-relationships :acl :used-by)))
    (is (= {:view #{:acl}} (type-rel/get-all-relationships type-relationships :has-many)))
    (is (= {:user #{:zone} :view #{:rrset} :rrset #{:rrweight}} (type-rel/get-all-relationships type-relationships :own)))
    ))

(deftest type-relationships-error-test
  (testing "type relationship parameter error"
    (ex/catch
       (fn [msg module]
          (is (= msg "duplicate relationship has-many or own"))
          (is (= module :resource-relationship)))
        (-> (type-rel/mk-resource-relationship)
          (type-rel/declare-relationship :owned-by :own)
          (type-rel/declare-relationship :has-many :own)))

    (ex/catch
       (fn [msg module]
          (is (= msg "unknown relationship has-many1"))
          (is (= module :resource-relationship)))
        (-> (type-rel/mk-resource-relationship)
          (type-rel/declare-relationship :owned-by :own)
          (type-rel/add-relationship! types :view :has-many1 :acl)))

    (ex/catch
       (fn [msg module]
          (is (= msg "unknown type view1"))
          (is (= module :resource-tree)))
        (-> (type-rel/mk-resource-relationship)
          (type-rel/declare-relationship :has-many :used-by)
          (type-rel/add-relationship! types :view1 :has-many :acl)))
   
    (ex/catch
       (fn [msg module]
          (is (= msg "acl -> view has has-many cycle"))
          (is (= module :resource-relationship)))
        (-> (type-rel/mk-resource-relationship)
          (type-rel/declare-relationship :has-many :used-by)
          (type-rel/add-relationship! types :view :has-many :acl)
          (type-rel/add-relationship! types :acl :has-many :view)))))
