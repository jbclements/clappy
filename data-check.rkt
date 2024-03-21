#lang racket

;; checking to see which enrollment data we have and which we don't, by comparing the FAD
;; with the enrollment data.

(require "make-db-connection.rkt"
         db
         shelly/instructor-name-ids
         racket/match
         racket/format
         csse-scheduling/qtr-math
         csse-scheduling/canonicalize
         sugar
         json)

(define fad-conn (make-connection "fad"))
(define scheduling-conn (make-connection "scheduling"))
(define progress-conn (make-connection "csseprogress"))

(define all-fad-courses
  (query-rows
   fad-conn
   "SELECT qtr,subject,num,section,enrollment FROM offerings;"))

(define all-enrollment-courses
  (query-rows
   progress-con
   "SELECT qtr,course,COUNT(*)"))
