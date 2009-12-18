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

(declare sleep-periodic-agent invoke-periodic-fn)

(defn- sleep-periodic-agent
  "Sleeps the periodic agent for :sleep-millis and then sends off invoke-periodic-fn."
  [state]
  (if (:active state)
    (do
      (Thread/sleep (:sleep-millis state))
      (send-off *agent* invoke-periodic-fn)))
  state)

(defn- invoke-periodic-fn
  "Performs one execution of a periodic agent.
Executes :periodic-fn, handles exceptions, increments :count, sleeps for :sleep-millis
and suspends the periodic agent if required."
  [state]
  (if (:active state)
    (try
     ((:periodic-fn state))
     (inc-count state)
     (catch Exception e
       (inc-count (store-exception state e)))
     (finally
      (send-off *agent* sleep-periodic-agent)))
    state))

(defn- periodic-agent
  "Creates a new periodic agent with its state initialized to an appropriate map."
  [func millis]
  (agent { :periodic-fn func          ; configurable
           :sleep-millis millis       ; configurable
           :count 0                   ; potential for overflow?
           :exception-count 0         ; potential for overflow?
           :recent-exceptions '()
           :max-recent-exceptions 10  ; should this be configurable?
           :active true               ; indirectly configurable
         }))

(defn periodically
  "Runs func every millis milliseconds using a periodic agent.
Returns the agent immediately."
  [func millis]
  (send-off (periodic-agent func millis) sleep-periodic-agent))

(defn suspend-periodic-agent
  "Suspends the repeated execution of a periodic agent."
  [agent]
  (send agent (fn [state]
                (assoc state :active false))))

(defn resume-periodic-agent
  "Resumes the repeated execution of a suspended periodic agent."
  [agent]
  (send agent (fn [state]
                (if (:active state)
                  state
                  (do
                    (send-off *agent* invoke-periodic-fn)
                    (assoc state :active true))))))

(defn periodic-agent->jmx [agent namespace agent-name]
  (jmx/register-mbean (jmx/Bean. agent)
                      (str namespace ":periodic-agent=" agent-name)))
