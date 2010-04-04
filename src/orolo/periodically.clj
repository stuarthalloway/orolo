;; Copyright (c) Glenn Vanderburg, 2009. All rights reserved.  The use
;; and distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(ns
    #^{ :author "Glenn Vanderburg"
        :doc "Easy, manageable, agent-based periodic background execution.

Usage
  (require 'orolo.periodically)

Starting periodic execution:
  ;; call hello every 3 seconds
  (periodically hello 3000)

  ;; call heartbeat every 30 seconds, and remember the agent in *heartbeat-agent*
  (defvar *heartbeat-agent* (periodically heartbeat (* 30 1000)))

Suspending:
  (suspend-periodic-agent *heartbeat-agent*)

Resuming:
  (resume-periodic-agent *heartbeat-agent*)

The periodically function creates and returns a type of agent known as a 'periodic agent'.
A periodic agent's state is a map with the following keys:

   :periodic-fn              the user-supplied function to be executed periodically
   :sleep-millis             the number of milliseconds between executions of periodic-fn
   :count 0                  the number of times periodic-fn has been called
   :exception-count          the total number of exceptions that periodic-fn has thrown
   :recent-exceptions        list of most recent exceptions thrown by periodic-fn (newest first)
   :max-recent-exceptions    the maximum number of recent exceptions kept
   :active                   periodic-fn will only be executed by the agent while this is true
" }
  orolo.periodically
  (:import clojure.contrib.jmx.Bean)
  (:require [clojure.contrib.jmx :as jmx]))

;; Todo:
;; - optionally execute right away (although the caller can always just call func just before calling periodically)
;; - optionally register an MBean exposing state and suspend/resume operations
;; - add optional circuit-breaker functionality around exceptions
;; - add some sort of logging hooks

(defn- inc-in [coll key]
  (assoc coll key (inc (key coll))))

(defn- inc-count
  "Increments the value of :count in the state of a periodic agent."
  [state]
  (inc-in state :count))

(defn- store-exception
  "Stores e in :recent-exceptions in the state of a periodic agent.
Old exceptions are dropped to keep the size of :recent-exceptions stays under :max-recent-exceptions."
  [state e]
  (let [exception-queue (:recent-exceptions state)]
    (inc-in
     (assoc state :recent-exceptions
            (conj (if (= (count exception-queue) (:max-recent-exceptions state))
                    (butlast exception-queue)
                    exception-queue)
                  e))
     :exception-count)))

(defn- invoke-periodic-fn
  "Performs one execution of a periodic agent.
Executes :periodic-fn, handles exceptions, increments :count, sleeps for :sleep-millis
and suspends the periodic agent if required."
  [state-ref]
  (dosync
   (ref-set state-ref
            (try
             ((:periodic-fn @state-ref))
             (inc-count @state-ref)
             (catch Exception e
               (inc-count (store-exception @state-ref e)))))))

(defn- schedule-periodic-task [task]
   (let [millis (:sleep-millis @(:state-ref task))
         func (:periodic-fn @(:state-ref task))]
     (.scheduleAtFixedRate (:executor task)
                           (partial invoke-periodic-fn (:state-ref task))
                           millis
                           millis
                           java.util.concurrent.TimeUnit/MILLISECONDS)))

(defprotocol PeriodicTask
  (suspend [pa])
  (resume [pa])
  (jmx [pa namespace executor-name]))

(deftype ScheduledFuturePeriodicTask
  [state-ref future-ref executor]
  :as this

  PeriodicTask
  (suspend [] (dosync
               (when (:active @state-ref)
                 (.cancel @future-ref false)
                 (ref-set state-ref (assoc @state-ref :active false)))))

  (resume [] (dosync
              (when-not (:active @state-ref)
                (ref-set future-ref (schedule-periodic-task this))
                (ref-set state-ref (assoc @state-ref :active true)))))

  (jmx [namespace task-name]
       (jmx/register-mbean (jmx/Bean. state-ref)
                           (str namespace ":PeriodicTask=" task-name))))

(def *periodic-executor* (java.util.concurrent.ScheduledThreadPoolExecutor. 2))

(defn periodically [func millis]
  (let [state-ref (ref { :periodic-fn           func
                         :sleep-millis          millis
                         :count                 0
                         :exception-count       0
                         :recent-exceptions     `()
                         :max-recent-exceptions 10
                         :active                true })
        task (ScheduledFuturePeriodicTask state-ref (ref nil) *periodic-executor*)]
    (dosync
     (ref-set (:future-ref task) (schedule-periodic-task task)))
    task))
