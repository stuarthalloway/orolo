(ns orolo.classloader-test
  (:use clojure.test orolo.classloader)
  (:import java.net.URL))

;; delegation will behave differently on different VMs
;; so these tests are pretty soft
(deftest delegation-test
  (testing "returns a sequence of class loaders"
    (is (every? #(instance? ClassLoader %)
                (delegation))))
  (testing "clojure.lang delegates to at least the system and extensions loader"
    (is (>= (count (delegation (.getClassLoader clojure.lang.IFn)))
            2)))
  (testing "app code has a longer delegation than clojure core"
    (is (> (count (delegation (.getClassLoader (.getClass delegation))))
           (count (delegation (.getClassLoader clojure.lang.IFn)))))))

(deftest delegation-urls-test
  (testing "returns a bunch of urls"
    (is (every? #(instance? URL %)
                (delegation-urls)))))