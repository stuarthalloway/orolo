(ns orolo.periodically-test
  (:use clojure.test orolo.periodically))

(def tns 'orolo.periodically)

;; (deftest is-json?-test
;;   (testing "when accept headers are set to application/json"
;;     (is (= true (is-json? {"accept" "application/json"}))))
;;   (testing "when accept headers are set to anything else"
;;     (is (= false (is-json? {"accept" "anything else"})))))

(deftest inc-in-test
  (let [inc-in (ns-resolve tns 'inc-in)]
    (testing "when collection contains key with inc-able value"
      (let [coll {:foo 0 :bar 1}]
        (is (= 2 (:bar (inc-in coll :bar))))))))

(deftest inc-count-test
  (let [inc-count (ns-resolve tns 'inc-count)]
    (testing "when count exists in the state"
      (let [state {:count 3}]
        (is (= 4 (:count (inc-count state))))))))

(deftest store-exception-test
  (let [store-exception (ns-resolve tns 'store-exception)]
    (testing "when :recent-exceptions is empty"
      (let [old-state {:recent-exceptions '() :max-recent-exceptions 2 :exception-count 0}
            new-state (store-exception old-state 'foo)]
        (is (= '(foo) (:recent-exceptions new-state)))
        (is (= 1 (:exception-count new-state)))))
    (testing "when :recent-exceptions has room"
      (let [old-state {:recent-exceptions '(bar) :max-recent-exceptions 2 :exception-count 1}
            new-state (store-exception old-state 'foo)]
        (is (= '(foo bar) (:recent-exceptions new-state)))
        (is (= 2 (:exception-count new-state)))))
    (testing "when :recent-exceptions is full"
      (let [old-state {:recent-exceptions '(bar baz) :max-recent-exceptions 2 :exception-count 2}
            new-state (store-exception old-state 'foo)]
        (is (= '(foo bar) (:recent-exceptions new-state)))
        (is (= 3 (:exception-count new-state)))))))
