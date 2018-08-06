Capture Fixity
==============

This actor captures the fixity details for a file given a qualified URI and the
internally-defined means to resolve and inspect it. In the current implementation,
it accepts only agave-canonical URIs.

It interacts with the SD2E Data Catalog, which is currently a MongoDB database,
via methods defined in `datacatalog`. The `datacatalog` directory is a prototype
for common methods used by other Python applications to interact with the
Data Catalog and will be factored out into a standalone module in Q32018 (as
we are not making the mistake we did with reactors and agaveutils again)

