# Files

The files package is intended to be a simple interface to S3 and document service calls.

## Examples

Uploading documents:

```python
    with files.register_download_and_documents(
            url, files.extractors.text_pdf) as fil:
        record['download_id'] = fil.download_id
        if fil.document_ids:
            record['document'] = fil.documents_ids[0]
```

Alternate format:

```python
    with files.request_file_with_cache(url) as fil:
        fil.upload_and_register()
        fil.extract_and_register(files.extractors.text_pdf)
        record['download_id'] = fil.download_id
        if fil.document_ids:
            record['document'] = fil.documents_ids[0]
```

Filtering downloads by mimetype:

```python
    with files.request_file_with_cache(url) as fil:
        if fil.mimetype in accepted_mimetypes:
            fil.upload_and_register()
```

Using files from alternate sources:

```python
    url = 'ftp://{}@{}/{}'.format(user, server, path)
    with ftp.get(path) as fp:
        fil = files.File(url, fp, filename=os.path.basename(path))
        fil.upload_and_register()
```

Multiple document creation (state regs):

```python
    with files.download_and_register(url) as fil:
        is_complete = False
        for entity in fil.extract(files.extractors.text_pdf):
            # parse entities, set is_complete=True when ready
            if is_complete:
                fil.add_document(doc)
                is_complete = False
        fil.register_documents()
        for document_id in fil.document_ids:
            record.add_document(document_id, "partial")
```

## Logical Layout

Most of the top-level functions are to be found in the `__init__.py` file. The `session.py` defines session specific implementations while `file.py` defines the `File` class. 

### __init__.py - Top level functions 

The call layout is designed to be a pyramid where complex functions are composed of lower level functions.

 - register_download_and_documents (File)
   - download_and_register (File)
     - request_file_with_cache (File)
        - request_file (File)
     - upload_and_register (download id)
        - upload_to_s3 (S3 URL)
        - create_download (download id)
   - extract_and_register_documents (File)
     - extract_and_parse (documents)
        - extract (extractor result)
        - parse (documents)
     - register_documents (File)

### file.py - File class

The `File` class is designed to handle the developing state of files, downloads, and documents through the pipeline. It includes the file pointer (`file_obj`), file source information (`url`, `is_cached`, `ldi` aka last-download-information, `source` - requests.Response or other objects), and document service state information.

### session.py - Session class

The `Session` class handles information about connections and settings.