(ns orolo.classloader
  (:use [clojure.contrib.seq-utils :only (flatten)])
  (:import java.net.URLClassLoader))

(defn delegation
  "Returns a sequence containing the classloader delegation from
   a given classloader, or from the context loader if none is
   specified."
  ([]
     (delegation (.getContextClassLoader (Thread/currentThread))))
  ([loader]
     (take-while
      (complement nil?)
      (iterate #(.getParent %) loader))))

(defn delegation-urls
  "Returns all the urls visible to the delegation, taking the
   same args as delegation."
  ([& args]
     (flatten
      (remove nil? (map
                    #(if (instance? URLClassLoader %) (seq (.getURLs %)) nil)
                    (apply delegation args))))))