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

- For standard versioned buckets on a real Amazon S3 endpoint, the only
  If-None-Match value supported for conditional PUT requests is '*',
  which requires that a non-deleted version of the object must not
  already exist. 

