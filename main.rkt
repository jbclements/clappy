#lang racket

(require "make-db-connection.rkt"
         db
         shelly/instructor-name-ids
         racket/match
         racket/format
         csse-scheduling/qtr-math
         sugar
         json)

(define fad-conn (make-connection "fad"))
(define scheduling-conn (make-connection "scheduling"))
(define progress-conn (make-connection "csseprogress"))

(define query-datum
  #;(hash 'instructor "ayaank"
        'course "csc203"
        'target-course "csc357")
  (hash 'instructor "clements"
        'course "csc202"
        'target-course "csc203"))

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

(define min-enroll-qtr
  (query-value
   progress-conn
   "SELECT MIN(qtr) FROM enroll;"))

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
  ;; fad checking is more or less superfluous, I believe, except as a check.
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


  (define rows
    (query-rows
     fad-conn
     "SELECT qtr,subject,num,section FROM offerfacs WHERE instructor = $1 AND qtr >= $2;"
     fad-name
     min-enroll-qtr))

  (define fad-course-offering-rows
    (filter
     (λ (row)
       (match-define (vector qtr subj num _) row)
       (set-member? mappings (list (qtr->catalog-cycle qtr) subj num)))
     rows))

  (define enroll-keys (list->set (map enroll-key enrolls)))

  (define fad-offerings
    (list->set
     (map (λ (row)
            (match-define (vector qtr subj num section) row)
            (list qtr section))
          fad-course-offering-rows)))

  
  (unless (set=? fad-offerings enroll-keys)
    (eprintf "fad minus enrolls: ~e\n"
             (set-subtract fad-offerings enroll-keys))
    (eprintf "enrolls minus fad: ~e\n"
             (set-subtract enroll-keys fad-offerings))
    (error 'fad-check "fad-offerings and enroll-keys not the same")))




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

(define students
  (list->set (map pq-student oneversion-self-check-data)))

(printf "~v students in this set\n"
        (set-count students))

;; we want to eliminate students that took the source course again from another
;; instructor.

;; first, we need a quick map of student/qtr. We're assuming here that
;; students can't take the course twice in the same quarter from different instructors.
;; this would be a problem if the lab (say) had a different instructor. Since
;; these are src-qtr, they took the course from the correct instructor

(define qtr/student-pairs
  (list->set
   (map (λ (pq) (list (pq-src-qtr pq) (pq-student pq)))
        oneversion-self-check-data)))



;; which students did not take the source course again later from another instructor?
(define non-last-takers
  (list->set
   (map
    pq-student
    (filter
     (λ (pq) (and (< (pq-src-qtr pq) (pq-tgt-qtr pq))
                  (not (set-member? (list (pq-tgt-qtr pq) (pq-student pq))
                                    qtr/student-pairs))))
     oneversion-self-check-data))))

(define last-takers
  (set-subtract students non-last-takers))

(printf "~v students after eliminating those who took it again later from another instructor\n"
        (set-count last-takers))


(define paired-check-data
  (time
  (query-rows
   progress-conn
   paired-query-string
   instructor-id
   course-id
   target-course-id
   )))

(define oneversion-paired-check-data
  (collapse-pq-rows paired-check-data))

(printf "~v recorded enrolled-grades in later course, only majors, counting repeats n times\n"
        (length oneversion-paired-check-data))

(define-values (rows-from-last-takers nltrs)
  (partition
   (λ (row) (set-member? last-takers (pq-student row)))
   oneversion-paired-check-data))

(printf "discarding ~v rows from students who took the source course again later.\n"
        (length nltrs))

;; ooh, let's discard those who took the target course *before* taking the source course
(define-values (not-later-taken later-taken)
  (partition (λ (pq) (<= (pq-tgt-qtr pq) (pq-src-qtr pq)))
             rows-from-last-takers))

(printf "discarding ~v rows from enrollments in the tgt course that didn't happen after the source course."
        (length not-later-taken))

(define tgt-qtrs
  (list->set (map pq-tgt-qtr later-taken)))

(printf "target quarters: ~v\n" tgt-qtrs)

(define all-target-grades-in-target-qtrs
  (query-rows
   progress-conn
   "SELECT qtr,grade,COUNT(*) FROM course_grade WHERE course=$1 GROUP BY qtr,grade;"
   target-course-id))

(define all-students-hash
  (make-immutable-hash
   (map
    (λ (g)
      (cons (string->symbol (vector-ref (first g) 1)) ; grade
            (apply + (map (λ (r) (vector-ref r 2)) g)) ;; count of those who received the grade
            ))
    (group-by
     (λ (summary-row) (vector-ref summary-row 1))
     (filter
      (λ (summary-row)
        (set-member? tgt-qtrs (vector-ref summary-row 0)))
      all-target-grades-in-target-qtrs)))))

;; okay now lets reduce by id, tgt-qtr (necessary to eliminate repeat-grades and also not
;; to double-count people who took the source course twice from the same instructor
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

(define id-tgt-qtr-grades
  (collapse-pq-rows-2 later-taken))

;; just collapsing all quarters together for now:
(define your-students-hash
  (frequency-hash
   (map string->symbol (map second id-tgt-qtr-grades))))

(displayln
(jsexpr->string
 (hash-set
 (hash-set query-datum
           'your-students your-students-hash)
 'all-students all-students-hash)))

;; separated by quarter
#;(map
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
