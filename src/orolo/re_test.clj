(ns orolo.re-test
  (:use [clojure.contrib.with-ns :only (with-ns)])
  (:require clojure.test))

(defmacro re-test [test-sym]
  `(do
     (require :reload-all '~test-sym)
     (clojure.test/run-tests '~test-sym)))

