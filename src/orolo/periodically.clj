;; Copyright (c) Glenn Vanderburg, 2009. All rights reserved.  The use
;; and distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(ns
    #^{ :author "Glenn Vanderburg"
        :doc "Easy, manageable, periodic background execution.

Usage
  (require 'orolo.periodically)

Starting periodic execution:
  ;; call hello every 3 seconds
  (periodically hello 3000)

  ;; call heartbeat every 30 seconds, and remember the task in *heartbeat-task*
  (defvar *heartbeat-task* (periodically heartbeat (* 30 1000)))

Suspending:
  (suspend *heartbeat-task*)

Resuming:
  (resume *heartbeat-task*)

Expose as JMX MBean:
  (jmx *heartbeat-task* \"MyService\" \"heartbeat\")

The periodically function creates and returns a value of the type PeriodicTask." }
  orolo.periodically
  (:require [clojure.contrib.jmx :as jmx]))

;; Todo:
;; - optionally execute right away (although the caller can always just call func just before calling periodically)
;; - add optional circuit-breaker functionality around exceptions
;; - add some sort of logging hooks

(defn- inc-in [coll key]
  (assoc coll key (inc (key coll))))

(defn- inc-count
  "Increments the value of :count in the state of a PeriodicTask."
  [state]
  (inc-in state :count))

(defn- store-exception
  "Stores e in :recent-exceptions in the state of a PeriodicTask.
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

(defn- update-in-task [task field new-value]
  "Updates a field in the state-ref of a PeriodicTask."
  (ref-set (:state-ref task)
           (assoc @(:state-ref task) field new-value)))

(defn- invoke-periodic-fn
  "Performs one execution of a PeriodicTask.
Executes :periodic-fn, handles exceptions, and increments :count."
  [state-ref]
  (dosync
   (ref-set state-ref
            (try
             ((:periodic-fn @state-ref))
             (inc-count @state-ref)
             (catch Exception e
               (inc-count (store-exception @state-ref e)))))))

(defn- schedule-periodic-task
  "Schedules a PeriodicTask to run on *periodic-executor*."
  [task]
   (let [millis (:sleep-millis @(:state-ref task))
         func (:periodic-fn @(:state-ref task))]
     (.scheduleAtFixedRate (:executor task)
                           (partial invoke-periodic-fn (:state-ref task))
                           millis
                           millis
                           java.util.concurrent.TimeUnit/MILLISECONDS)))

(defprotocol PeriodicTask
  "A function scheduled to run periodically."
  (suspend [pa])
  (resume [pa])
  (jmx [pa namespace executor-name]))

(definterface PeriodicTaskMXBean
  ;; getters
  (#^short          getMaxNumberOfRecentExceptionsKept [])
  (#^long           getNumberOfExceptions [])
  (#^long           getNumberOfTimesRun [])
  (#^String         getPeriodicFunction [])
  (#^java.util.List getRecentExceptions [])
  (#^long           getSleepMillis [])

  ;; queries
  (#^boolean isActive [])

  ;; setters
  (#^void setMaxNumberOfRecentExceptionsKept [#^short num])
  (#^void setSleepMillis [#^long millis])

  ;; operations
  (#^void suspend [])
  (#^void resume [])
  (#^void clearRecentExceptions []))

(deftype ScheduledFuturePeriodicTaskMXBean [task]

  orolo.periodically.PeriodicTaskMXBean
  (getMaxNumberOfRecentExceptionsKept [] (:max-recent-exceptions @(:state-ref task)))
  (getNumberOfExceptions [] (:exception-count @(:state-ref task)))
  (getNumberOfTimesRun [] (:count @(:state-ref task)))
  (getPeriodicFunction []
     (let [func (:periodic-fn @(:state-ref task))
           func-meta (meta (:periodic-fn @(:state-ref task)))
           ns (:ns func-meta)
           name (:name func-meta)]
       (cond
        (and ns name) (str ns "/" name)
        name name
        true (.toString func))))
  (getRecentExceptions [] (map #(.toString %) (:recent-exceptions @(:state-ref task))))
  (getSleepMillis [] (:sleep-millis @(:state-ref task)))

  (isActive [] (:active @(:state-ref task)))

  (setMaxNumberOfRecentExceptionsKept [num]
     (dosync
      (update-in-task task :max-recent-exceptions num)))
  (setSleepMillis [millis]
     (dosync
      (update-in-task task :sleep-millis millis)
      (when (:active @(:state-ref task))
        (do
          (suspend task)
          (resume task)))))

  (suspend []
     (when (:active @(:state-ref task))
       (suspend task)))
  (resume []
     (when-not (:active @(:state-ref task))
       (resume task)))
  (clearRecentExceptions [] (dosync (update-in-task task :recent-exceptions '()))))

(deftype ScheduledFuturePeriodicTask
  [state-ref future-ref executor]
  :as this

  PeriodicTask
  (suspend []
     (dosync
      (when (:active @state-ref)
        (.cancel @future-ref false)
        (ref-set state-ref (assoc @state-ref :active false)))))

  (resume []
     (dosync
      (when-not (:active @state-ref)
        (ref-set future-ref (schedule-periodic-task this))
        (ref-set state-ref (assoc @state-ref :active true)))))

  (jmx [namespace task-name]
     (jmx/register-mbean (ScheduledFuturePeriodicTaskMXBean this)
                         (str namespace ":PeriodicTask=" task-name))))

(def *periodic-executor* (java.util.concurrent.ScheduledThreadPoolExecutor. 2))

(defn periodically
  [func millis]
  (let [state-ref (ref { :active                true
                         :count                 0
                         :exception-count       0
                         :max-recent-exceptions 10
                         :periodic-fn           func
                         :recent-exceptions     `()
                         :sleep-millis          millis })
        task (ScheduledFuturePeriodicTask state-ref (ref nil) *periodic-executor*)]
    (dosync
     (ref-set (:future-ref task) (schedule-periodic-task task)))
    task))
