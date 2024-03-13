#lang racket

(require "make-db-connection.rkt"
         db
         shelly/instructor-name-ids
         racket/match
         racket/format
         csse-scheduling/qtr-math
         sugar)

(define fad-conn (make-connection "fad"))
(define scheduling-conn (make-connection "scheduling"))
(define progress-conn (make-connection "csseprogress"))

(define query-datum
  (hash 'instructor "ayaank"
        'course "csc203"
        'target-course "csc357"))

(define instructor-id (hash-ref query-datum 'instructor))
(define course-id (hash-ref query-datum 'course))
(define target-course-id (hash-ref query-datum 'target-course))

;; given a >= comparator and a nonempty list, return the element
;; a such that a >= all elements in the list. If called with a
;; non-reflexive > operator, return an element such that no other
;; element is > it
(define (maxcomp comparator l #:key [key-finder (λ (x) x)])
  (let loop ([best (first l)]
             [best-key (key-finder (first l))]
             [remaining (rest l)])
    (cond [(empty? remaining) best]
          [else (cond [(comparator best-key (key-finder (first remaining)))
                       (loop best best-key (rest remaining))]
                      [else
                       (loop (first remaining) (key-finder (first remaining))
                             (rest remaining))])])))

;; are the elements of l all the same? error on empty list
(define (all-the-same? l)
  (define f (first l))
  (let loop ([elts (rest l)])
    (cond [(empty? elts) #t]
          [else (cond [(equal? f (first elts)) (loop (rest elts))]
                      [else #f])])))

(module+ test
  (require rackunit)
  (check-equal? (all-the-same? '(1 1 1 1)) #t)
  (check-equal? (all-the-same? '(1 1 1 2)) #f)
  (check-equal? (all-the-same? '(2 1 1 1)) #f)

  (check-equal? (maxcomp (lambda (a b) (>= (string-length a) (string-length b)))
                         '("a" "bc" "def" "c" "ghi" "j"))
                "def")
  (check-equal? (maxcomp (lambda (a b) (> (string-length a) (string-length b)))
                         '("a" "bc" "def" "c" "ghi" "j"))
                "ghi"))

(define fad-names (id->fad-names (string->symbol instructor-id)))
(define fad-name
  (match fad-names
    [(list fn) fn]
    [other (error 'fad-name "expected exactly one hit, got: ~e" other)]))

;; because of cross-listings, we have to look in many different places
;; for the offerings of this course; it may be that this course was cross-listed
;; in one catalog but not in another, for instance.
(define mappings
  (map
   (λ (row)
     (match-define (vector cc subj 3num) row)
     (list cc subj (string-append "0" 3num)))
   (query-rows
    scheduling-conn
    "SELECT cycle,subject,num FROM course_mappings WHERE id=$1"
    (hash-ref query-datum 'course))))

;; if this is slow, optimize it by making only one db query...
;; (listof (vector qtr section)
#;(apply
 append
 (for/list ([m (in-list mappings)])
   (match-define (vector cc subj num _) m)
   ;; making the bold assumption that these quarters are uninterrupted...
   (define cc-qtrs (catalog-cycle->qtrs cc))
   (define min-qtr (apply min cc-qtrs))
   (define max-qtr (apply max cc-qtrs))
   (query-rows
    fad-conn
    (~a "SELECT qtr,section FROM offerfacs WHERE instructor = $1 AND subject=$2 AND num=$3 "
        " AND qtr >= $4 AND qtr <= $5;")
    fad-name
    subj (~a "0" num)
    min-qtr max-qtr
    )))


(define rows
  (query-rows
   fad-conn
   "SELECT qtr,subject,num,section FROM offerfacs WHERE instructor = $1;"
   fad-name))

(define course-offering-rows
  (filter
   (λ (row)
     (match-define (vector qtr subj num _) row)
     (set-member? mappings (list (qtr->catalog-cycle qtr) subj num)))
   rows))

(define enrolls
  (time
  (query-rows
   progress-conn
   (~a
    "SELECT qtr,sect,id FROM enroll WHERE instructor = $1 "
    " AND course = $2;")
   instructor-id
   course-id
   )))

(printf "~v enrolls, including nonmajors and counting retakers n times\n" (length enrolls))
(printf "oh and also counting lab and lecture separately")

(define (enroll-key row)
  (list
   (vector-ref row 0)
   (vector-ref row 1)))

;; check that the fad and the enrolls database come up with the same sections.
(let ()
  (define grouped (group-by enroll-key enrolls))
  (define tallys-from-progress-db
    (map (λ (g) (list (enroll-key (first g))
                      (length g)))
         grouped))
  (define fad-offerings
    (list->set
     (map (λ (row)
            (match-define (vector qtr subj num section) row)
            (list qtr section))
          course-offering-rows)))
  (unless (set-equal? (set-subtract fad-offerings (list->set (map first tallys-from-progress-db))))
    (error 'abc)))

start-transaction


;; ooh... just do it all on the DB side, in one query?

;; wow, this works fine. Okay, for now we'll just focus on one target course.
#;(define all-data
  (time
  (query-rows
   progress-conn
   (~a
    "SELECT e.qtr,e.sect,e.id,cg.qtr,cg.course,cg.grade,cg.version FROM "
    "  (enroll e inner join course_grade cg ON e.id = cg.id)"
    " WHERE e.instructor = $1 AND e.course = $2;")
   instructor-id
   course-id
   )))

(define paired-query-string
  (~a
   "SELECT e.qtr,e.sect,e.id,cg.qtr,cg.grade,cg.version FROM "
   "  (enroll e inner join course_grade cg ON e.id = cg.id)"
   " WHERE e.instructor = $1 AND e.course = $2 AND cg.course = $3;"))

;; does the student show up as having taken the source course in course_grade?
;; (if not, presumably not a major)
(define self-check-data
  (time
  (query-rows
   progress-conn
   paired-query-string
   instructor-id
   course-id
   course-id
   )))

;; all but the grade & version: (could be faster, probably)
(define (pq-datum row) (take (vector->list row) 4))
(define (pq-src-qtr row) (vector-ref row 0))
(define (pq-student row) (vector-ref row 2))
(define (pq-tgt-qtr row) (vector-ref row 3))
(define (pq-grade row) (vector-ref row 4))
(define (pq-version row) (vector-ref row 5))
;; given a listof data from the paired query, collapse over versions, choose the
;; most recent version if necessary.
(define (collapse-pq-rows pq-rows)
  ;; collapse over versions, choose the most recent version if necessary
  (define grouped (group-by pq-datum pq-rows))
  (for/list ([g (in-list grouped)])
    (cond [(all-the-same? (map pq-grade g)) (apply vector
                                                   (take (vector->list (first g)) 5))]
          [else
           (define f (first g))
           (define latest (maxcomp string>? #:key pq-version g))
           (eprintf "warning: student ~v in qtr ~v sees grades ~v, ends on ~v\n"
                    (pq-student f)
                    (pq-tgt-qtr f)
                    (map pq-grade g)
                    (pq-grade latest))
           (apply vector (take (vector->list latest) 5))])))

(define oneversion-self-check-data
  (collapse-pq-rows self-check-data))

(printf "~v recorded enrolled-gradeds, only CSC/CPE/EE majors[*], counting repeats n times\n"
        (length oneversion-self-check-data))

(printf "should we only count people that got passing grades from this instructor?\n")

(define paired-check-data
  (time
  (query-rows
   progress-conn
   paired-query-string
   instructor-id
   course-id
   target-course-id
   )))

;; ooh, let's discard those who took the target course *before* taking the source course
(define-values (not-later-taken later-taken)
  (partition (λ (pq) (<= (pq-tgt-qtr pq) (pq-src-qtr pq)))
             paired-check-data))

(define oneversion-paired-check-data
  (collapse-pq-rows later-taken))

(printf "~v recorded enrolled-grades in later course, only majors, counting repeats n times\n"
        (length oneversion-paired-check-data))

;; okay now lets reduce by id, tgt-qtr
(define (pq-key-2 row) (list (pq-student row) (pq-tgt-qtr row)))
(define (collapse-pq-rows-2 pq-rows)
  ;; collapse over versions, choose the most recent version if necessary
  (define grouped (group-by pq-key-2 pq-rows))
  (for/list ([g (in-list grouped)])
    ;; discarding id here
    (cond [(all-the-same? (map pq-grade g)) (list (pq-tgt-qtr (first g))
                                                  (pq-grade (first g)))]
          [else
           (define f (first g))
           (define latest (maxcomp string>? #:key pq-version g))
           (eprintf "warning: student ~v in qtr ~v sees grades ~v, ends on ~v\n"
                    (pq-student f)
                    (pq-tgt-qtr f)
                    (map pq-grade g)
                    (pq-grade latest))
           (list (pq-tgt-qtr (first g) (pq-grade (first g))))])))

(map
 (λ (g)
   (list (first (first g))
         (frequency-hash (map second g))))
(sort
 (group-by
  first
  (collapse-pq-rows-2 oneversion-paired-check-data))
 <
 #:key (compose first first)
 ))



#;(for/list ([hit (in-list hits)])
  (match-define (vector qtr subj num section) hit)
  )
