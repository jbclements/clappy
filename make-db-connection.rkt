#lang racket/base

(require db
         memoize
         racket/contract
         racket/match
         racket/file)

;; this file provides the function that makes a connection to the
;; grading database, and also provides a parameter that allows a program
;; to specify its own connection-maker

(provide
 (contract-out
  ;; return a new db connection using the current connection-maker
  [make-connection (-> string? connection?)]
  ;; the parameter that contains the function to be used when making a connection
  [connection-maker (parameter/c
                     (-> string? connection?))]
  ;; a replacement connection-maker for use directly on desmond
  [local-connect (-> string? connection?)]
  [tcp-connect (-> string? connection?)]))

(define local-db-info-file
  "/Users/clements/.ssh/clappy-db-info.rktd")

;; the user/password to use for the connection
(define-values (db-user db-password)
  (cond [(file-exists? local-db-info-file)
         (match (file->value local-db-info-file)
           [(list (list 'user db-user) (list 'password db-passwd))
            (values db-user db-passwd)])]
        [else
         (error 'db-user-passwd
                "db credentials file not found: ~e"
                local-db-info-file)]))
;; the database to connect to
(define DATABASE "fad")


;; connect to a database using tcp
(define (tcp-connect db)
  (postgresql-connect
   #:user db-user
   #:password db-password
   #:port 13432
   #:database db))

;; connect to the local database
(define (local-connect db)
  (postgresql-connect #:user db-user
                      #:password db-password
                      #:port 5432
                      #:database db))

;; the parameter that controls how connections are made.
(define connection-maker
  (make-parameter tcp-connect))

;; make a connection by calling the current value of the connection-maker parameter
(define/memo (make-connection db)
  ((connection-maker) db))
