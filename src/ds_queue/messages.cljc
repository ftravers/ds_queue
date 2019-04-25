(ns ds-queue.messages
  #?(:clj
     (:require
      [clojure.core.async :refer
       [chan pub tap sub >! <! unsub-all go go-loop]
       :as async])
     :cljs
     (:require
      [cljs.core.async :refer
       [chan pub tap sub >! <! unsub-all]
       :as async]
      [cljs.core.async :refer-macros [go go-loop]])))

#?(:cljs (enable-console-print!))
(defn init-queues []
  {:global-write-queue nil
   :write-queue-mult nil
   :publications []})

(def queue-state
  (atom (init-queues)))

(defn prn-state []
  (let [{:keys [publications
                write-queue-mult
                global-write-queue]} @queue-state
        pub-count (count publications)]
    {:global-write-queue
     (if global-write-queue :active nil)
     :write-queue-mult
     (if write-queue-mult :active nil)
     :publications
     (if (> pub-count 0)
       (str pub-count " subscriptions")
       "no subscriptions")}))

(declare pred-n-do
         global-write-queue
         pub-msg
         not-nil?
         make-watch-pub
         write-queue-mult)

(defn pub-msg
  "send a message on the global write queue"
  [msg]
  (let [write-queue
        (-> @queue-state :global-write-queue)]
    (if write-queue (do
                      (go (>! write-queue msg))
                      msg)
        "global write queue not initialized yet!")))

(defn start []
  (let [gwq (chan)]
    (reset! queue-state
            (-> @queue-state
                (assoc :global-write-queue gwq)
                (assoc :write-queue-mult (async/mult gwq))))))

(defn stop []
  (let [{:keys [publications
                write-queue-mult
                global-write-queue]} @queue-state]
    (map unsub-all publications)
    (if write-queue-mult
      (async/untap-all write-queue-mult))
    (if global-write-queue
      (async/close! global-write-queue)))
  (reset! queue-state (init-queues)))

(defn- make-watch-pub
  "Create queue we can monitor with *watch-predicate*"
  [watch-predicate]
  (let [write-mult (-> @queue-state :write-queue-mult)
        the-tap (tap write-mult (chan))]
    (pub the-tap watch-predicate)))

(defn- pred-to-sub
  "given a predicate, create a pub and subscribe to it. RETURNS: the
  subscribed channel to read from and a function to unsubscribe from
  the created pub queue."
  [watch-predicate]
  (let [pub-queue (make-watch-pub watch-predicate)
        msg-chan (chan)]
    (sub pub-queue true msg-chan)
    [msg-chan pub-queue]))
(defn- initialized? []
  (let [{:keys [publications
                write-queue-mult
                global-write-queue]} @queue-state]
    (and global-write-queue write-queue-mult)))

;; TODO: CANT DEBUG CORE-ASYNC...known issue...
(defn watch-n-do!
  ">watch-predicate< is a function to run against message data, when it
  evaluates to true then run >action-fn<.  RETURNS: a function that
  when called closes the pub and anything sub'ed to it."
  [watch-predicate action-fn]
  (if (initialized?)
    (let [[msg-chan pub-queue]
          (pred-to-sub watch-predicate)]
      (reset! queue-state
              (-> @queue-state
                  ;; below dies:
                  (update :publications conj pub-queue)))
      (go-loop []
        (action-fn (<! msg-chan))
        (recur)))
    "system not initialized"))

(comment
  (watch-n-do! #(= :abc (:msg-type %))
               #(println (str ":abc watch " %)))

  (watch-n-do! #(= :def (:msg-type %))
               #(println (str ":def watch " %)))

  (watch-n-do! #(not-nil? %)
               #(println (str "Global Watcher: " %)))

  (pub-msg {:msg-type :abc :blah 123})
  (pub-msg {:msg-type :def :blah :blah})
  (pub-msg "abcdef"))
