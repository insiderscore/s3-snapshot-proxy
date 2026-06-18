# Unsupported Or Out-Of-Scope S3 API Families

This advisory lane is scoped to the object behavior of proxy-backed writable
clone buckets. The proxy expects origin buckets and the shared overlay bucket to
exist before clients use the proxy.

These upstream `s3-tests` families are intentionally excluded from the initial
allowlist:

- Bucket administration: create bucket, delete bucket, list buckets, bucket
  location, bucket ownership controls.
- Bucket versioning administration: origin and overlay buckets must already
  have versioning enabled; the shim provisions this at the fixture boundary.
- IAM, STS, users, tenants, account policy, role policy, session policy.
- Bucket ACLs, object ACLs, object ownership, bucket policies, anonymous access.
- CORS, website, lifecycle, bucket logging, notifications.
- ListMultipartUploads owner/ACL variants. Basic bucket-level listing is
  supported with virtual-bucket prefix rewriting, but owner-specific behavior
  depends on ACL support.
- Multipart edge cases outside the initial allowlist: duplicate
  CompleteMultipartUpload after a successful completion, and concurrent
  same-part resend ordering. The MinIO-backed overlay reports NoSuchUpload or
  InvalidPart for those upstream cases, so they need an explicit support-matrix
  decision before inclusion.
- CopyObject and UploadPartCopy. These require source-object rewriting across
  the virtual bucket view.
- Multi-object delete variants that depend on object-lock governance behavior
  or per-entry conditional fields such as ETag, LastModifiedTime, and Size.
  Basic ListObjects/ListObjectsV2-visible batch deletes and overlay
  version-specific batch deletes are supported. Version IDs that exist only in
  the origin bucket return item-level failures because the proxy must never
  mutate origin bucket versions.
- Object tagging and bucket tagging.
- Server-side encryption and customer-provided encryption keys.
- S3 Select.
- Object lock and retention.
- Storage classes and transition/restore behavior.

Moving any family from this ledger into `allowlist.txt` should include a short
reason in the change and should not hide an observed failure without either a
product fix, an adapter fix, or an explicit support-matrix decision.
