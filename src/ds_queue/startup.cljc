(ns ds-queue.startup
  (:require
   [ds-queue.messages :as queues]
   [integrant.core :as ig]))

(def config
  {:queues/state {}})

(defmethod ig/init-key :queues/state
  [_ _]
  (queues/start))

(defonce system (atom (ig/init config)))

(defmethod ig/halt-key! :queues/state
  [_ _]
  (queues/stop)
  (reset! system nil))

(comment
  (ig/init config)
  (prn-str @system)
  (ig/halt! @system)
  (queues/pub-msg {:msg-type :abc :value "HELLO"})
  (prn-str @system)
  (queues/prn-state)
  (queues/watch-n-do!
   #(= :abc (:msg-type %))
   #(println (str ":abc watch message ==> " (:value %))))
  (queues/prn-state)
  (queues/pub-msg {:msg-type :abc :value "HELLO"})
  (queues/pub-msg {:msg-type :abc1 :value "HELLO"})
  (ig/halt! @system)
  (queues/pub-msg {:msg-type :abc :value "HELLO"})
  (queues/prn-state)
  (prn-str @system))

