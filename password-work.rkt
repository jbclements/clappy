#lang racket

(require crypto)
(require crypto/libcrypto)

(crypto-factories (list libcrypto-factory))

(define passwords
  '((akeen #"")
    (ayaank #"")
    (clements #"")))

(define pw-file-lines
  (for/list ([pr (in-list passwords)])
    (string-append (symbol->string (first pr))
                   "@" (pwhash 'scrypt (second pr) '((ln 19) (p 1) (r 8))))))

(call-with-output-file "/tmp/password-hashes.txt"
  (λ (port)
    (for ([a (in-list pw-file-lines)])
      (displayln a port))))