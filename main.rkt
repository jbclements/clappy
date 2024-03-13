#lang racket/base

(require "make-db-connection.rkt"
         db
         shelly/instructor-name-ids
         racket/match
         racket/format
         csse-scheduling/qtr-math)

(define fad-conn (make-connection "fad"))
(define scheduling-conn (make-connection "scheduling"))

(define query-datum
  (hash 'instructor "ayaank"
        'course "csc203"))

(define fad-names (id->fad-names (string->symbol (hash-ref query-datum 'instructor))))
(define fad-name
  (match fad-names
    [(list fn) fn]
    [other (error 'fad-name "expected exactly one hit, got: ~e" other)]))

;; because of cross-listings, we have to look in many different places
;; for the offerings of this course; it may be that this course was cross-listed
;; in one catalog but not in another, for instance.
(define mappings
  (query-rows
   scheduling-conn
   "SELECT * FROM course_mappings WHERE id=$1"
   (hash-ref query-datum 'course)))

;; if this is slow, optimize it by making only one db query...
(for/list ([m (in-list mappings)])
  (match-define (vector cc subj num _) m)
  ;; making the bold assumption that these quarters are uninterrupted...
  (define cc-qtrs (catalog-cycle->qtrs cc))
  (define min-qtr (apply min cc-qtrs))
  (define max-qtr (apply max cc-qtrs))
  (query-rows
   fad-conn
   (~a "SELECT * FROM offerfacs WHERE instructor = $1 AND subject=$2 AND num=$3 "
       " AND qtr >= $4 AND qtr <= $5;")
   fad-name
   subj num
   min-qtr max-qtr))


(define rows
  (query-rows
   fad-conn
   "SELECT * FROM offerfacs WHERE instructor = $1;"
   fad-name))
