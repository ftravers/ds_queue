(ns ds-queue.core
    (:require
     [rum.core :refer [defc mount] :as rum]))

  (defc hello []
    [:div "Hello!"])

  (defn main-page [comp]
    (mount
     (comp)
     (js/document.getElementById "app")))

  (main-page hello)
