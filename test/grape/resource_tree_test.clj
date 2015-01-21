(ns grape.resource-tree-test
  (:require [clojure.test :refer :all]
            [grape.resource-tree :as tree] 
            [grape.exception :as ex]))

(def resource-tree
  (-> (tree/mk-resource-tree [:root])
      (tree/declare-type-hierarchy [[:view :zone :rrset] [:user] [:acl] [:rrweight]])))

(deftest type-tree-test
  (testing "test type tree, usage, fields"
      (is (= '(:view) (tree/type-ancesters resource-tree :zone)))
      (is (= true (tree/is-leaf-type? resource-tree :acl)))
      (is (= false (tree/is-leaf-type? resource-tree  :zone)))
      (is (= false (tree/is-leaf-type? resource-tree :unknown)))
      (is (= 6 (count (tree/types resource-tree ))))
      (is (= '() (tree/type-ancesters resource-tree :view)))
      (is (= '(:view :zone :rrset) (tree/type-chain resource-tree :rrset)))
      (is (= '(:view :zone) (tree/type-ancesters resource-tree :rrset)))))
