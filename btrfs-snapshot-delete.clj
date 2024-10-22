#!/usr/bin/env bb
;****************************************************************************** 
; This file is free software: you can redistribute it and/or modify it under 
; the terms of the GNU General Public License as published by the Free Software
; Foundation, either version 3 of the License, or (at your option) any later 
; version.
;
; This program is distributed in the hope that it will be useful, but WITHOUT 
; ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
; FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
;
; See <https://www.gnu.org/licenses/>.
;
; Copyright (c) 2024 Jens Hofer
;
; SPDX-License-Identifier: GPL-3.0-or-later
;******************************************************************************

(require '[babashka.process :refer [shell]]) 
(require '[babashka.cli :as cli]) 
(require '[clojure.pprint :refer [pprint]]) 
(require '[clojure.set :as s]) 
(require '[clojure.edn :as edn]) 

(import (java.time OffsetDateTime)
        (java.time.format DateTimeFormatter))

(def creation-date-formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss ZZ"))
(def day-date-formatter (DateTimeFormatter/ofPattern "yyyyMMdd"))
(def month-date-formatter (DateTimeFormatter/ofPattern "yyyyMM"))
(def year-date-formatter (DateTimeFormatter/ofPattern "yyyy"))

(def cli-spec
  {:spec
   {:path {:coerce :string
           :desc "Path of btrfs filesystem"
           :alias :p
           :require true}
    :configuration {:coerce :string
                    :desc "Configuration for retension in map form"
                    :alias :c
                    :require false}
    :dry-run {:coerce :boolean
              :desc "Only create logs - no deletion"
              :alias :d
              :require true}
    :verbose {:coerce :boolean
              :desc "Writes out verbose log files"
              :alias :v
              :require false}
    :sudo {:coerce :boolean
           :desc "Run btrfs commands with sudo"
           :alias :s
           :require: false}}
   :error-fn
   (fn [{:keys [spec type cause msg option] :as data}]
     (when (= :org.babashka/cli type)
       (case cause
         :require
         (println
           (format "Missing required argument: %s\n" option))
         :validate
         (println
           (format "%s does not exist!\n" msg)))
       (throw (ex-info msg data)))
     (System/exit 1))})

; (def opts {:path "/home/mau/btrfs-disk"
;            :configuration "{\"btrfs-disk\" {:day2day 2 :first-of-day 5 :first-of-month 2 :first-of-year 10}}"})
; (def opts {:path "/home/mau/btrfs-disk" :dry-run false})
(def opts
  (cli/parse-opts *command-line-args* cli-spec))

(def btrfs-path
  (if-let [path (:path opts)]
    path
    "/"))

(def dry-run
  (if (nil? (:dry-run opts))
    true
    (:dry-run opts)))

(def verbose
  (if (nil? (:verbose opts))
    true
    (:verbose opts)))

(def sudo
  (if (nil? (:sudo opts))
    true
    (:sudo opts)))

(def configuration
  (if-let [conf (edn/read-string (:configuration opts))]
    conf
    (if-not dry-run 
      (do (println "No configuration and no dry-run is too dangerous!")
        (System/exit 1))
        nil))) 

(defn btrfs-show
  [subvol-id path]
  (if sudo
    (:out (shell {:out :string} "sudo btrfs subvolume show -r " subvol-id path))
    (:out (shell {:out :string} "btrfs subvolume show -r " subvol-id path))))

(defn btrfs-list
  [path]
  (if sudo
    (:out (shell {:out :string} "sudo btrfs subvolume list -t -s " path))
    (:out (shell {:out :string} "btrfs subvolume list -t -s " path))))

(defn btrfs-delete
  [subv-id path]
  (if sudo
    (:out (shell {:out :string} "sudo btrfs subvolume delete -i " subv-id path))
    (:out (shell {:out :string} "btrfs subvolume delete -i " subv-id path))))

(defn parse-datetime
  [dt-str]
  (OffsetDateTime/parse dt-str creation-date-formatter))

(defn volumn-from-name
  [sname]
  (if-let [pt-pos (str/last-index-of sname ".")]
    (subs sname 0 pt-pos)
    nil))

(defn conj-not-empty
  [coll v]
  (if (or (nil? v)
          (empty? v))
    coll
    (apply conj coll v)))

(defn create-time-map
  [time-str]
  (let [ctime (parse-datetime time-str)]
    (-> {:creation-time ctime}
        (conj {:day (.format ctime day-date-formatter)})
        (conj {:month (.format ctime month-date-formatter)})
        (conj {:year (.format ctime year-date-formatter)}))))


(defn clean-line-vec
  [lvec]
  (let [noempty (filterv #(> (count (str/trim %)) 0) lvec)]
    (reduce #(conj %1 (str/trim %2)) [] noempty))) 

(defn line2map
  [line]
  (let [cline (clean-line-vec (str/split line #"\t"))]
    (if (seq cline)
      {(first cline) (rest cline)})))

(defn cmd-out2map
  ([out]
   (cmd-out2map out 0))
  ([out from]
  (let [lsplit (drop from (str/split-lines out))]
    (reduce #(conj %1 (line2map %2)) {} lsplit))))

(defn create-show-map
  [show-res]
  (-> {:name (first (get show-res "Name:"))}
      (conj {:volume (volumn-from-name (first (get show-res "Name:")))})
      (conj {:uuid (first (get show-res "UUID:"))})
      (conj {:subv-id (parse-long (first (get show-res "Subvolume ID:")))})
      (conj (create-time-map (first (get show-res "Creation time:"))))))

(defn create-snapshot-map
  [path]
  (let [all-snaps (cmd-out2map (btrfs-list path) 2)]
    (reduce #(conj %1 (create-show-map (cmd-out2map (btrfs-show %2 path)))) 
            [] 
            (keys all-snaps))))

(defn create-volumn-list
  ([vol-map]
   (create-volumn-list vol-map configuration))
  ([vol-map config]
    (let [vol-name (:volume (first vol-map))
          vol-config (get config vol-name {:day2day 2 :first-of-day 5 :first-of-month 2 :first-of-year 10})
          day-first (reduce #(conj %1 (first (val %2))) #{} (group-by :day (sort-by :creation-time vol-map)))
          day-rest (reduce #(conj-not-empty %1 (rest (val %2))) [] (group-by :day (sort-by :creation-time vol-map)))
          month-first (reduce #(conj %1 (first (val %2))) [] (group-by :month (sort-by :creation-time vol-map)))
          year-first (reduce #(conj %1 (first (val %2))) [] (group-by :year (sort-by :creation-time vol-map)))
          del-day2day (into #{} (filterv #(.isBefore (:creation-time %) 
                                           (.minusDays (OffsetDateTime/now) (:day2day vol-config))) day-rest))
          not-del-day (into #{} (filterv #(.isAfter (:creation-time %) 
                                           (.minusDays (OffsetDateTime/now) (:first-of-day vol-config))) day-first))
          not-del-month (into #{} (filterv #(.isAfter (:creation-time %) 
                                             (.minusMonths (OffsetDateTime/now) (:first-of-month vol-config))) month-first))
          not-del-year (into #{} (filterv #(.isAfter (:creation-time %) 
                                            (.minusYears (OffsetDateTime/now) (:first-of-year vol-config))) year-first))]
      {:day-first day-first
       :day-rest day-rest
       :month-first month-first
       :year-first year-first
       :del-day2day del-day2day
       :not-del-day not-del-day
       :not-del-month not-del-month
       :not-del-year not-del-year
       :del-longterm (s/difference day-first not-del-day not-del-month not-del-year)
       :vol-name vol-name
       :vol-config vol-config})))

(defn log-volumen-all
  [vol-map]
  (with-open [w (clojure.java.io/writer "volumen-log-all.edn" :append true)]
    (pprint vol-map w)))

(defn log-volumen-delete
  [vol-map]
  (with-open [w (clojure.java.io/writer "volumen-log-delete.edn" :append true)]
    (pprint (:del-day2day vol-map) w)
    (pprint (:del-longterm vol-map) w)))

(defn some-col
  [value col]
  (some #(= % value) col))

(defn remove-vols-noconfig
  [vols]
  (if-let [vol-with-config (keys configuration)]
    (filterv #(some-col % vol-with-config) vols)
    vols)) 

(defn log-snapshots-map
  [vol-map]
  (doseq [snapshot vol-map]
    (println "WOULD be gone: " (:name snapshot) " ID: "(:subv-id snapshot) " from " btrfs-path)))

(defn delete-snapshots-map
  [vol-map]
  (doseq [snapshot vol-map]
    (println "Deleting now " (:name snapshot) " ID: "(:subv-id snapshot) " from " btrfs-path)
    (btrfs-delete (:subv-id snapshot) btrfs-path)))

(let [snap-map (create-snapshot-map btrfs-path)
      snap-map-volumes (group-by :volume snap-map)
      filtered-map-vols (remove-vols-noconfig (keys snap-map-volumes))]
  (if (seq snap-map-volumes)
    (doseq [vol-key filtered-map-vols]
      (let [vol-map (create-volumn-list (get snap-map-volumes vol-key))]
        (if verbose
          (do (log-volumen-all vol-map)
              (log-volumen-delete vol-map)))
        (if dry-run 
          (do 
            (println "WOULD be deleting day2day:")
            (log-snapshots-map (:del-day2day vol-map))
            (println "WOULD be deleting long-term:")
            (log-snapshots-map (:del-longterm vol-map)))
          (do
            (println "Deleting day2day:")
            (delete-snapshots-map (:del-day2day vol-map))
            (println "Deleting long-term:")
            (delete-snapshots-map (:del-longterm vol-map)))))))) 


