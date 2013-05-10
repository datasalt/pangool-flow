; This is work-in-progress playground for integration Clojure with pangool-flow
; TODO for some reason multi-level namespaces don't work (understand why)
(ns test
  (:require clojure.data.json)
  (:gen-class))

(import 'com.datasalt.pangool.flow.MRStep)
(import 'com.datasalt.pangool.flow.GroupBy)
(import 'com.datasalt.pangool.flow.ResourceMRInput)
(import 'com.datasalt.pangool.flow.MapReduceFlowBuilder)
(import 'com.datasalt.pangool.flow.BaseFlow)
; inner classes -> $ !
(import 'com.datasalt.pangool.flow.BaseFlow$EXECUTION_MODE)
(import 'com.datasalt.pangool.flow.io.TextInput)
(import 'com.datasalt.pangool.flow.io.TextOutput)
(import 'com.datasalt.pangool.flow.mapred.TextMapper)
; why can't we use .* ?
(import 'com.datasalt.pangool.flow.mapred.TupleOpReducer)
(import 'com.datasalt.pangool.flow.ops.ClojureOp)
(import 'com.datasalt.pangool.flow.ops.TopTuples)

(import 'org.apache.hadoop.conf.Configuration)

; http://dev.clojure.org/display/design/Where+Did+Clojure.Contrib+Go
; https://github.com/clojure/data.json/

(def test-tweet "{ \"created_at\":\"created-at-foo\", \"text\":\"text-foo\", \"user\": { \"screen_name\":\"screen-name-foo\", \"location\":\"location-foo\" } }")

(def tweet-schema "date:string?,text:string,screen_name:string,location:string?")
(def flow-input (new ResourceMRInput "src/test/resources/barackobama_tweets.json"))
(def hadoop-conf (new Configuration))
(def execution-mode (BaseFlow$EXECUTION_MODE/OVERWRITE))

(defn tweet-parse-op [input]
  (let [parsed-tweet (clojure.data.json/read-json input)]
    (list 
      (:created_at parsed-tweet) 
      (:text parsed-tweet) 
      (:screen_name (:user parsed-tweet))
      (:location (:user parsed-tweet)))))

(defn str-array [str]
  (into-array [str]))

(defn create-step []
  (let [step (new MRStep "step-1" (Class/forName "test"))
        richInput (new TextInput (new TextMapper (new ClojureOp tweet-parse-op tweet-schema)))] ; this is *seriously* ugly
    (doto step 
      (.addInput flow-input richInput) 
      (.groupBy (new GroupBy (str-array "screen_name")))
      (.setReducer (new TupleOpReducer (new TopTuples 5))))))

(defn create-flow []
  (let [step (create-step)
        out (.setOutput step (new TextOutput))
        flow-builder (new MapReduceFlowBuilder)]
    (doto flow-builder 
      (.addInput flow-input)
      (.addStep step)
      (.bindOutput out "out-clj"))))

(defn run-flow []
  (doto (create-flow) (.execute execution-mode hadoop-conf (str-array "out-clj"))))