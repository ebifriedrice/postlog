(ns ebifriedrice.postlog.relations
  "Track the relations a stream announces and turn positional tuples
  into row maps.

  pgoutput identifies tables by OID and sends column values
  positionally, with a Relation message (see
  [[ebifriedrice.postlog.pgoutput]]) arriving before the first change
  that references a table — and again whenever its definition
  changes.  A consumer keeps the latest relation per OID in a map and
  resolves changes against it.

  Key-only before-images (updates that changed a replica-identity
  column, and deletes on tables without REPLICA IDENTITY FULL)
  carry values for the key columns only, in relation column order
  restricted to those columns; full tuples carry every column.

  Rows are maps from column name (string) to value, because column
  names come from the database and stay data here."
  (:require [ebifriedrice.postlog.pgoutput :as pg]))

(defn apply-message
  "Fold a decoded Relation message into the relation cache."
  [cache {:keys [relation-id] :as relation}]
  (if (= :relation (:op relation))
    (assoc cache relation-id relation)
    cache))

(defn resolve
  "The cached relation for `relation-id`, or ex-info.  A stream is
  malformed if a change references a relation it never announced."
  [cache relation-id]
  (or (get cache relation-id)
      (throw (ex-info "Change references a relation the stream never announced"
                      {:relation-id relation-id
                       :known (sort (keys cache))}))))

(defn- row
  "Zip a tuple with the columns it carries values for: all columns for
  full tuples, only key columns for key-only tuples."
  [{:keys [columns]} kind tuple]
  (let [cols (if (= :key kind) (filter :key? columns) columns)]
    (when (not= (count cols) (count tuple))
      (throw (ex-info "Tuple width does not match its relation"
                      {:columns (mapv :name cols)
                       :tuple tuple})))
    (zipmap (map :name cols) tuple)))

(defn shape-change
  "Reshape a decoded insert/update/delete/truncate message (resolved
  against `cache`) into the change maps this library delivers:

  ```clojure
  {:op :insert :schema \"public\" :table \"users\" :after {\"id\" \"1\"}}
  {:op :update ... :before {\"id\" \"1\"} :after {\"id\" \"2\"}}
  {:op :delete ... :before {\"id\" \"1\"}}
  {:op :truncate :tables [\"public.users\" ...] :cascade? false
   :restart-identity? false}
  ```

  `:before` is nil unless the server sent an old image; on default
  replica identity it holds key columns only.  TOASTed values the
  server skipped appear as [[pg/unchanged-toast]]."
  [cache change]
  (case (:op change)
    :insert (let [rel (resolve cache (:relation-id change))]
              {:op :insert
               :schema (:schema rel)
               :table (:table rel)
               :after (row rel :new (:tuple change))})
    :update (let [rel (resolve cache (:relation-id change))]
              {:op :update
               :schema (:schema rel)
               :table (:table rel)
               :before (when-let [t (:before-tuple change)]
                         (row rel (:before-kind change) t))
               :after (row rel :new (:tuple change))})
    :delete (let [rel (resolve cache (:relation-id change))]
              {:op :delete
               :schema (:schema rel)
               :table (:table rel)
               :before (row rel (:before-kind change) (:before-tuple change))})
    :truncate {:op :truncate
               :tables (mapv (fn [id]
                               (let [rel (resolve cache id)]
                                 (str (:schema rel) "." (:table rel))))
                             (:relation-ids change))
               :cascade? (:cascade? change)
               :restart-identity? (:restart-identity? change)}
    change))
