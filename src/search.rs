use anyhow::{Context, Result};
use crate::classify::Classification;
use crate::{InfoHash, TorrentMetadata};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexReader, IndexWriter, Order, ReloadPolicy, TantivyDocument, Term};

const SCHEMA_VERSION: &str = "2";

pub struct SearchIndex {
    index: Index,
    reader: Arc<RwLock<IndexReader>>,
    writer: Arc<RwLock<IndexWriter>>,
    reload_on_commit: bool,
    schema: Schema,
    info_hash_field: Field,
    name_field: Field,
    files_field: Field,
    content_type_field: Field,
    tags_field: Field,
    total_length_field: Field,
    seeders_field: Field,
    leechers_field: Field,
    enriched_at_field: Field,
    discovered_at_field: Field,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SearchHit {
    pub info_hash: String,
    pub name: String,
    pub content_type: String,
    pub tags: Vec<String>,
    pub total_length: u64,
    pub seeders: u64,
    pub leechers: u64,
    pub enriched_at: u64,
    pub score: f32,
}

#[derive(Clone, Debug, Default)]
pub struct SearchQuery {
    pub q: String,
    pub content_type: Option<String>,
    pub tags: Vec<String>,
    pub offset: usize,
    pub limit: usize,
    pub sort: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub total: u64,
}

impl SearchIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, 50, true)
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        writer_heap_mb: usize,
        reload_on_commit: bool,
    ) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = build_schema();
        let version_path = path.join(".schema_version");
        if path.join("meta.json").exists() {
            let existing = std::fs::read_to_string(&version_path).unwrap_or_default();
            if existing.trim() != SCHEMA_VERSION {
                std::fs::remove_dir_all(path).ok();
            }
        }
        let created = !path.join("meta.json").exists();
        let index = if path.join("meta.json").exists() {
            Index::open_in_dir(path).context("open search index")?
        } else {
            std::fs::create_dir_all(path)?;
            Index::create_in_dir(path, schema.clone()).context("create search index")?
        };
        if created {
            std::fs::write(&version_path, SCHEMA_VERSION)?;
        }
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .context("create index reader")?;
        let info_hash_field = schema.get_field("info_hash").unwrap();
        let name_field = schema.get_field("name").unwrap();
        let files_field = schema.get_field("files").unwrap();
        let content_type_field = schema.get_field("content_type").unwrap();
        let tags_field = schema.get_field("tags").unwrap();
        let total_length_field = schema.get_field("total_length").unwrap();
        let seeders_field = schema.get_field("seeders").unwrap();
        let leechers_field = schema.get_field("leechers").unwrap();
        let enriched_at_field = schema.get_field("enriched_at").unwrap();
        let discovered_at_field = schema.get_field("discovered_at").unwrap();
        let writer = index
            .writer(writer_heap_mb.max(1) * 1_000_000)
            .context("open index writer")?;
        Ok(Self {
            index,
            reader: Arc::new(RwLock::new(reader)),
            writer: Arc::new(RwLock::new(writer)),
            reload_on_commit,
            schema,
            info_hash_field,
            name_field,
            files_field,
            content_type_field,
            tags_field,
            total_length_field,
            seeders_field,
            leechers_field,
            enriched_at_field,
            discovered_at_field,
        })
    }

    pub fn with_writer<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut IndexWriter) -> Result<T>,
    {
        let mut writer = self
            .writer
            .write()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        f(&mut writer)
    }

    pub fn commit_if_needed(&self, pending: &mut usize, commit_batch: usize) -> Result<()> {
        if *pending >= commit_batch {
            self.with_writer(|writer| {
                writer.commit()?;
                Ok(())
            })?;
            if self.reload_on_commit {
                self.reload()?;
            }
            *pending = 0;
        }
        Ok(())
    }

    pub fn reload(&self) -> Result<()> {
        self.reader
            .write()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .reload()
            .context("reload index reader")
    }

    pub fn upsert(
        &self,
        writer: &mut IndexWriter,
        info_hash: &InfoHash,
        metadata: &TorrentMetadata,
        classification: &Classification,
        seeders: u64,
        leechers: u64,
        enriched_at: u64,
        discovered_at: u64,
    ) -> Result<()> {
        let term = Term::from_field_text(self.info_hash_field, &info_hash.to_hex());
        writer.delete_term(term);
        let files_text = crate::classify::flatten_file_paths(metadata);
        let tags_text = classification.tags.join(" ");
        let document = doc!(
            self.info_hash_field => info_hash.to_hex(),
            self.name_field => metadata.name.clone(),
            self.files_field => files_text,
            self.content_type_field => classification.content_type.clone(),
            self.tags_field => tags_text,
            self.total_length_field => metadata.total_length,
            self.seeders_field => seeders,
            self.leechers_field => leechers,
            self.enriched_at_field => enriched_at,
            self.discovered_at_field => discovered_at,
        );
        writer.add_document(document)?;
        Ok(())
    }

    pub fn update_swarm_stats(
        &self,
        info_hash: &InfoHash,
        seeders: u64,
        leechers: u64,
    ) -> Result<bool> {
        let reader = self
            .reader
            .read()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .clone();
        let searcher = reader.searcher();
        let term = Term::from_field_text(self.info_hash_field, &info_hash.to_hex());
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        let top_docs = searcher.search(
            &query,
            &TopDocs::with_limit(1).order_by_score(),
        )?;
        let Some((_score, doc_address)) = top_docs.into_iter().next() else {
            return Ok(false);
        };
        let retrieved: TantivyDocument = searcher.doc(doc_address)?;
        let name = field_str(&retrieved, self.name_field);
        let files = field_str(&retrieved, self.files_field);
        let content_type = field_str(&retrieved, self.content_type_field);
        let tags = field_str(&retrieved, self.tags_field);
        let total_length = retrieved
            .get_first(self.total_length_field)
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let enriched_at = retrieved
            .get_first(self.enriched_at_field)
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let discovered_at = retrieved
            .get_first(self.discovered_at_field)
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        self.with_writer(|writer| {
            let term = Term::from_field_text(self.info_hash_field, &info_hash.to_hex());
            writer.delete_term(term);
            let document = doc!(
                self.info_hash_field => info_hash.to_hex(),
                self.name_field => name,
                self.files_field => files,
                self.content_type_field => content_type,
                self.tags_field => tags,
                self.total_length_field => total_length,
                self.seeders_field => seeders,
                self.leechers_field => leechers,
                self.enriched_at_field => enriched_at,
                self.discovered_at_field => discovered_at,
            );
            writer.add_document(document)?;
            Ok(())
        })?;
        Ok(true)
    }

    pub fn search(&self, query: &SearchQuery) -> Result<SearchResult> {
        let reader = self
            .reader
            .read()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .clone();
        let searcher = reader.searcher();
        let final_query = self.build_query(query)?;

        let total = searcher
            .search(&final_query, &Count)
            .context("count search results")? as u64;

        let limit = query.limit + query.offset;
        let mut hits = Vec::new();
        if total == 0 {
            return Ok(SearchResult { hits, total });
        }
        match query.sort.as_str() {
            "seeders" => {
                let top_docs = searcher.search(
                    &final_query,
                    &TopDocs::with_limit(limit).order_by_u64_field("seeders", Order::Desc),
                )?;
                for (_score, doc_address) in top_docs.into_iter().skip(query.offset) {
                    let retrieved: TantivyDocument = searcher.doc(doc_address)?;
                    hits.push(document_to_hit(&retrieved, &self.schema, 0.0)?);
                }
            }
            "enriched_at" => {
                let top_docs = searcher.search(
                    &final_query,
                    &TopDocs::with_limit(limit).order_by_u64_field("enriched_at", Order::Desc),
                )?;
                for (_score, doc_address) in top_docs.into_iter().skip(query.offset) {
                    let retrieved: TantivyDocument = searcher.doc(doc_address)?;
                    hits.push(document_to_hit(&retrieved, &self.schema, 0.0)?);
                }
            }
            _ => {
                let top_docs = searcher.search(
                    &final_query,
                    &TopDocs::with_limit(limit).order_by_score(),
                )?;
                for (score, doc_address) in top_docs.into_iter().skip(query.offset) {
                    let retrieved: TantivyDocument = searcher.doc(doc_address)?;
                    hits.push(document_to_hit(&retrieved, &self.schema, score)?);
                }
            }
        }
        Ok(SearchResult { hits, total })
    }

    fn build_query(&self, query: &SearchQuery) -> Result<Box<dyn tantivy::query::Query>> {
        let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();

        if !query.q.trim().is_empty() {
            let parser = QueryParser::for_index(
                &self.index,
                vec![self.name_field, self.files_field],
            );
            let parsed = parser
                .parse_query(query.q.trim())
                .context("parse search query")?;
            subqueries.push((Occur::Must, parsed));
        }

        if let Some(content_type) = &query.content_type {
            if !content_type.is_empty() {
                let term = Term::from_field_text(self.content_type_field, content_type);
                subqueries.push((
                    Occur::Must,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                ));
            }
        }

        for tag in &query.tags {
            if !tag.is_empty() {
                let term = Term::from_field_text(self.tags_field, tag);
                subqueries.push((
                    Occur::Must,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                ));
            }
        }

        Ok(if subqueries.is_empty() {
            Box::new(tantivy::query::AllQuery)
        } else {
            Box::new(BooleanQuery::new(subqueries))
        })
    }

    pub fn search_legacy(&self, query: &SearchQuery) -> Result<Vec<SearchHit>> {
        Ok(self.search(query)?.hits)
    }

    pub fn get_by_hash(&self, info_hash: &InfoHash) -> Result<Option<SearchHit>> {
        let reader = self
            .reader
            .read()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .clone();
        let searcher = reader.searcher();
        let term = Term::from_field_text(self.info_hash_field, &info_hash.to_hex());
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        let top_docs = searcher.search(
            &query,
            &TopDocs::with_limit(1).order_by_score(),
        )?;
        if let Some((score, doc_address)) = top_docs.into_iter().next() {
            let retrieved: TantivyDocument = searcher.doc(doc_address)?;
            return Ok(Some(document_to_hit(&retrieved, &self.schema, score)?));
        }
        Ok(None)
    }

    pub fn schema_version() -> &'static str {
        SCHEMA_VERSION
    }
}

fn field_str(document: &TantivyDocument, field: Field) -> String {
    document
        .get_first(field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn build_schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("info_hash", STRING | STORED);
    builder.add_text_field("name", TEXT | STORED);
    builder.add_text_field("files", TEXT);
    builder.add_text_field("content_type", STRING | STORED);
    builder.add_text_field("tags", TEXT | STORED);
    builder.add_u64_field("total_length", INDEXED | STORED);
    builder.add_u64_field(
        "seeders",
        NumericOptions::default().set_stored().set_fast(),
    );
    builder.add_u64_field("leechers", INDEXED | STORED);
    builder.add_u64_field(
        "enriched_at",
        NumericOptions::default().set_stored().set_fast(),
    );
    builder.add_u64_field("discovered_at", INDEXED | STORED);
    builder.build()
}

fn document_to_hit(
    document: &TantivyDocument,
    schema: &Schema,
    score: f32,
) -> Result<SearchHit> {
    let info_hash_field = schema.get_field("info_hash").unwrap();
    let name_field = schema.get_field("name").unwrap();
    let content_type_field = schema.get_field("content_type").unwrap();
    let tags_field = schema.get_field("tags").unwrap();
    let total_length_field = schema.get_field("total_length").unwrap();
    let seeders_field = schema.get_field("seeders").unwrap();
    let leechers_field = schema.get_field("leechers").unwrap();
    let enriched_at_field = schema.get_field("enriched_at").unwrap();

    let info_hash = document
        .get_first(info_hash_field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let name = document
        .get_first(name_field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let content_type = document
        .get_first(content_type_field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let tags = document
        .get_first(tags_field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_string)
        .collect();
    let total_length = document
        .get_first(total_length_field)
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let seeders = document
        .get_first(seeders_field)
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let leechers = document
        .get_first(leechers_field)
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let enriched_at = document
        .get_first(enriched_at_field)
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    Ok(SearchHit {
        info_hash,
        name,
        content_type,
        tags,
        total_length,
        seeders,
        leechers,
        enriched_at,
        score,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classification_config::ClassificationRules;
    use crate::classify::classify;
    use tempfile::TempDir;

    #[test]
    fn empty_index_sort_seeders_returns_empty() {
        let dir = TempDir::new().unwrap();
        let search = SearchIndex::open(dir.path()).unwrap();
        let result = search
            .search(&SearchQuery {
                q: String::new(),
                content_type: None,
                tags: vec![],
                offset: 0,
                limit: 1,
                sort: "seeders".into(),
            })
            .unwrap();
        assert_eq!(result.hits.len(), 0);
        assert_eq!(result.total, 0);
    }

    #[test]
    fn indexes_and_searches_by_name() {
        let dir = TempDir::new().unwrap();
        let search = SearchIndex::open(dir.path()).unwrap();
        let rules = ClassificationRules::load("config/classification.toml").unwrap();
        let hash = InfoHash([1u8; 20]);
        let metadata = TorrentMetadata {
            name: "Ubuntu Desktop 22.04".into(),
            files: vec![],
            total_length: 4_000_000_000,
        };
        let classification = classify(&metadata, &rules);
        search
            .with_writer(|writer| {
                search.upsert(
                    writer,
                    &hash,
                    &metadata,
                    &classification,
                    10,
                    2,
                    1_700_000_000,
                    1_699_999_000,
                )?;
                writer.commit()?;
                Ok(())
            })
            .unwrap();
        search.reload().unwrap();

        let result = search
            .search(&SearchQuery {
                q: "Ubuntu".into(),
                content_type: None,
                tags: vec![],
                offset: 0,
                limit: 10,
                sort: "score".into(),
            })
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].info_hash, hash.to_hex());
        assert!(result.total >= 1);
    }

    #[test]
    fn sorts_by_seeders_descending() {
        let dir = TempDir::new().unwrap();
        let search = SearchIndex::open(dir.path()).unwrap();
        let rules = ClassificationRules::load("config/classification.toml").unwrap();
        let low = InfoHash([30u8; 20]);
        let high = InfoHash([31u8; 20]);
        search
            .with_writer(|writer| {
                for (hash, name, seeders) in [
                    (low, "low-seeders", 2u64),
                    (high, "high-seeders", 50u64),
                ] {
                    let metadata = TorrentMetadata {
                        name: name.into(),
                        files: vec![],
                        total_length: 100,
                    };
                    let classification = classify(&metadata, &rules);
                    search.upsert(
                        writer,
                        &hash,
                        &metadata,
                        &classification,
                        seeders,
                        1,
                        1_700_000_000,
                        1_699_999_000,
                    )?;
                }
                writer.commit()?;
                Ok(())
            })
            .unwrap();
        search.reload().unwrap();

        let result = search
            .search(&SearchQuery {
                q: String::new(),
                content_type: None,
                tags: vec![],
                offset: 0,
                limit: 10,
                sort: "seeders".into(),
            })
            .unwrap();
        assert_eq!(result.total, 2);
        assert_eq!(result.hits[0].info_hash, high.to_hex());
        assert_eq!(result.hits[0].seeders, 50);
    }
}
