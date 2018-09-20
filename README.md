Capture Uploads Fixity
======================

This actor captures the fixity details for a file given a qualified URI and the
internally-defined means to resolve and inspect it. In the current implementation,
it accepts only agave-canonical URIs.

It interacts with the SD2E Data Catalog, a MongoDB database, using via methods
defined in the [datacatalog](https://github.com/SD2E/python-datacatalog) Python
package.

Example Fixity Record
---------------------

This is an example of a MongoDB record for a specific file.

```json
{
    "_id" : ObjectId("5b8ac5c78f0954000118c3de"),
    "filename" : "transcriptic/201808/yeast_gates/r1bsmgdayg2yq_r1bsu7tb7bsuk/6388_0.00015_2.fcs",
    "uuid" : BinData(3, "V4m97g6vUnO6JKkwvKM2hA=="),
    "properties" : {
        "lab" : "Transcriptic",
        "checksum" : "29370e2ac6ec246637a813e312ff7e6be4fb9285",
        "modified_date" : ISODate("2018-09-10T01:50:20.527+0000"),
        "revision" : 1,
        "size" : 8905548,
        "original_filename" : "transcriptic/201808/yeast_gates/r1bsmgdayg2yq_r1bsu7tb7bsuk/6388_0.00015_2.fcs",
        "file_modified" : ISODate("2018-08-28T01:15:49.000+0000"),
        "file_created" : ISODate("2018-08-28T01:15:49.000+0000"),
        "created_date" : ISODate("2018-09-01T17:00:55.109+0000"),
        "file_type" : "text/plaintext"
    }
}
```

**Explanation:** The `filename` is relative  to `/uploads/` on the
`data-sd2e-community` resource. Each indexed file has a  unique identifier
`uuid` which is a hash of `filename`.

The creation and update times for the index are recorded as (`created_date` and
 `modified_date`), while the apparent* creation and update dates for the
physical file are stored as (`file_created`  and `file_updated`). The `size`
(in bytes) and `checksum` are computed from the  physical file, and  `lab` is
inferred from the filename. The value for `file_type` is determined using the
Python file_types package and is always a MIME type. Finally, `revision`
indicates how many times the index has been refreshed.
