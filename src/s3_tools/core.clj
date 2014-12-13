(ns s3-tools.core
  (:require [aws.sdk.s3 :as s3]
            [clojure.core.async :as a]))

(defn- adjust-options
  [options new-marker n-read]
  (-> (if (contains? options :max-keys)
        (update-in options [:max-keys] - n-read)
        options)
    (assoc :marker new-marker)))

(defn- object-seq*
  [cred bucket options xs]
  (if (seq xs)
    (cons (first xs)
          (lazy-seq (object-seq* cred bucket options (rest xs))))
    (let [{:keys [next-marker objects]}
          (s3/list-objects cred bucket options)
          n-objects (count objects)]
      (if (seq objects)
        (recur cred
               bucket
               (adjust-options options next-marker n-objects)
               objects)
        nil))))

(defn object-seq
  "Returns a lazy sequence of objects, accepts the same options as
  `aws.sdk.s3/list-objects`, whose docs you should read and understand
  in order to properly use. The sequence returned will page through
  multiple results if necessary."
  [cred bucket & [options]]
  (object-seq* cred bucket options nil))

(defn object-chan
  "Returns an S3-object-producing channel, accepts the same options as
  `aws.sdk.s3/list-objects`, whose docs you should read and understand
  in order to properly use. The channel returned will page through
  multiple results if necessary."
  [cred bucket & [options]]
  (let [ch (a/chan)]
    (a/go-loop [xs (object-seq cred bucket options)]
      (if (seq xs)
        (do (a/>! ch (first xs))
            (recur (rest xs)))
        (a/close! ch)))))
