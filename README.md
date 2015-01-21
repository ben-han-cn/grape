# grape
A Clojure library privide a simple and lightweight database persistent layer.

Grape provides a simple declarative interface to express the models and 
the relationship between them, then generate a store to handle all the 
model CRUD operation.

## Usage
``` clojure
(-> (mk-resource-descriptor)
    (declare-type-hierarchy [:teacher] [:classroom :student])
    (descriptor/declare-resource :teacher {:fields {:name :string :major :string} 
                                           :id-fields [:name]})
    (descriptor/declare-resource :classroom {:fields {:name :string} 
                                           :id-fields [:name]})
    (descriptor/declare-resource :student {:fields {:name :string :age :integer} 
                                           :has-many [:teacher]
                                           :id-fields [:name]}))

(add-resource :teacher {:name "Tom" :major "math"})
(add-resource :teacher {:name "Henrry" :major "physics"})
(add-resource :classroom {:name "l1"})
(add-resource :student {:name "Jerry" :age 10 :teachers ["Tom" "Henrry"] :classroom "l1"})

(get-resources :student {:name "Jerry"}) ;  {:name "Jerry" :age 10 
                                         ;   :teachers ["Tom" "Henrry"] :classroom "l1"}

(del-resource :teacher {:name "Tom"}); will be rejected since it is used by Jerry
(update-resource :student {:name "Jerry"} {:teachers ["Henrry"]}); 
(del-resource :teacher {:name "Tom"}); now Tom will be deleted since no one refer to it.
(del-resource :classroom {:name "l1"}) ; will delete the classroom and Jerry
``` 

## Design
Typically, one application manages severl related models. Model has attributes.
Grape handle two kind of relationship, owned-by and has-many.

Model and its relationship is decleared and understood by grape through resource 
descriptor. Then grape use the descriptor to generate the SQL schema. Each model 
will saved into one table, and has id field.

When A owned-by B which means, when B is deleted A will be deleted too. And when
create A there must has a B exists.
When A has-many B, which means there is a one/many to many relationship between A
and B.
Grape uses foreign keys to handle owned-by relationship, and use a join table to handle
the many to many relationship.

FIXME
## License

Copyright © 2014 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
