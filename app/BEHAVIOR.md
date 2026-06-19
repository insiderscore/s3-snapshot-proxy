# Proxy Behavior

This proxy intends to provide a read-through, write-aside cache for S3
style object storage, which functions similarly to a LVM snapshot on a
filesystem. 

- Instead of hitting the https://s3.amazonaws.com/ or minio endpoints
directly, clients will hit them through this proxy.

- The proxy will simulate a writable snapshot of each origin bucket.

- Changes to objects in the origin bucket which happen after START_TIME
will be hidden from proxy clients.

- Changes to bucket contents made by proxy users will be recorded in the
  overlay bucket, not any of the origin buckets, and will be visible
  only to other proxy users.

- Since the proxy does not have write access to origin buckets, it is
  incapable of making any changes which would be visible to direct users
  of those buckets via their normal endpoint.

Pre-requisites:

- The origin buckets and the overlay bucket must have versioning enabled.

- The overlay bucket must not contain objects created before START_TIME

- The client must sign requests to the proxy with credentials which
  would allow it to make read-only requests to the origin buckets. The
  proxy will not attempt to write to the origin buckets, but as a matter
  of best practice its credentials should not have write access to the
  origin bucket.

- The proxy itself must have credentials which allow it to make read-only
  requests to the origin bucket. These credentials are used when a
  request must be mutated in ways which would invalidate the original
  client signature.

- The proxy itself must have full read/write access to an overlay
  bucket, which it uses to store updated versions of objects including
  delete markers

Compatible handling of DELETE requests

- When running against a real Amazon S3 endpoint, a delete marker will
  be created for each DELETE request, even if there is no objects or
  current undeleted version for the target key. When running against
  minio, DELETE against a non-existent or already deleted key returns
  success but does not create a delete marker for already deleted or
  non-existent objects. Our overlay bucket could be stored on either s3
  or minio, so our proxy must cope with both implementations. 

- For standard versioned buckets on a real S3 endpoint, conditional
  DELETE requests return 501. On minio, preconditions on DELETE requests
  are silently ignored. Our proxy should not forward conditional DELETE
  requests and instead return 501.

- Multi-object delete is supported for ordinary object keys by applying the
  same overlay delete-marker compatibility behavior to each unversioned item
  in the request. The proxy rewrites virtual keys into the shared overlay
  bucket, deletes there, and rewrites the returned keys back to the virtual
  bucket view.

- Version-specific multi-object delete entries are only allowed to mutate
  versions in the overlay bucket. If the requested version ID exists only in
  the origin bucket, the proxy returns an item-level AccessDenied error and
  does not mutate the origin. Conditional multi-object delete fields such as
  ETag, LastModifiedTime, and Size are not currently implemented.

- For standard versioned buckets on a real Amazon S3 endpoint, the only
  If-None-Match value supported for conditional PUT requests is '*'. The
  value '*' means there must be no already existing non-deleted version
  with the same key.

Handling of ListObjects and ListObjectsV2:

- For a given combination of prefix and delimiter, the proxy will
  enumerate all object versions and delete markers from the origin
  bucket which should be visible as of START_TIME, plus any superseding
  objects or delete markers from the overlay bucket.

- Legacy ListObjects uses the same merged snapshot view as ListObjectsV2.
  It accepts marker-based pagination and returns marker/NextMarker XML
  fields rather than the v2 continuation-token fields.

- A delete marker for a given key will render all previous versions for
  that same key invisible. 

- Due a shortcoming in the v2 S3 API as implemented by boto3, we must
  rely on the lastModified timestamp to tell which versions should be
  invisible as a result of a delete marker in the origin bucket. While
  the REST endpoint returns a single chronically ordered list of
  versions and delete markers, the API wrapper breaks them into two
  separate lists. When recombining into a single ordered list, the
  timestamp precision is insufficient to determine the order of
  operations which collide on the same second. In this condition, we
  return the first (newest) object from the collision. (FIXME: We can
  avoid this problem by hitting the REST endpoint directly and parsing
  the returned XML)

Handling of multipart uploads:

- Object-level multipart upload operations are write-aside operations
  against the overlay bucket. Initiate, upload part, list parts,
  complete, and abort requests are forwarded to the overlay object key
  under the virtual bucket prefix.

- Multipart upload subresources do not fall back to the origin bucket.
  Upload state exists only in the overlay bucket, and an aborted upload
  must not create a delete marker.

- Bucket-level ListMultipartUploads is served from the overlay bucket with
  the virtual bucket name applied as an overlay prefix. Returned keys,
  common prefixes, key markers, and next key markers are rewritten back to
  the virtual bucket view so in-progress uploads for other virtual buckets
  in the shared overlay bucket are not exposed.

- CopyObject and UploadPartCopy are intentionally rejected with 501
  because they require source-object rewriting across the virtual
  bucket view.

Handling of server-side encryption:

- Explicit SSE-S3 object uploads are passed through to the overlay bucket by
  forwarding `x-amz-server-side-encryption: AES256`. The overlay backend must
  be configured to support SSE-S3.

- Explicit KMS key selection and customer-provided encryption keys are not
  currently forwarded. Default bucket encryption is expected to be transparent
  when configured directly on origin or overlay buckets, but bucket encryption
  administration is outside the proxy's support surface.

Handling of object tagging:

- Object tagging operations are passed through to the overlay bucket for objects
  that already exist in the overlay view. This includes tags supplied during
  multipart upload initiation.

- Tagging objects that exist only in the origin bucket is not handled by the
  overlay-only pass-through path.
