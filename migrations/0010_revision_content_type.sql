-- Older versions only recorded the document's current MIME type. Preserve the
-- type they were served with at upgrade time. Future writes record the exact
-- type alongside the bytes, even if another version has a different type.
alter table document_revisions add column if not exists content_type text;

select set_config('app.workspace', '*', true);
update document_revisions r
set content_type = d.content_type
from documents d
where r.document_id = d.id and r.content_type is null;
