use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
pub struct ClassificationFile {
    pub blocklist: Vec<String>,
    #[serde(default)]
    pub formats: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub tags: Vec<TagRuleConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TagRuleConfig {
    #[serde(rename = "type")]
    pub tag_type: String,
    pub includes: Option<String>,
    pub regex: Option<String>,
    pub tag: String,
}

pub struct ClassificationRules {
    blocklist: Vec<String>,
    formats: HashMap<String, Vec<String>>,
    tags: Vec<CompiledTagRule>,
}

struct CompiledTagRule {
    tag_type: String,
    includes: Option<String>,
    regex: Option<Regex>,
    tag: String,
}

impl ClassificationRules {
    pub fn load(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("read classification config {:?}", path.as_ref()))?;
        let file: ClassificationFile = toml::from_str(&content)?;
        Self::from_file(file)
    }

    pub fn from_file(file: ClassificationFile) -> Result<Arc<Self>> {
        let mut tags = Vec::new();
        for rule in file.tags {
            let regex = if let Some(pattern) = rule.regex {
                Some(Regex::new(&pattern).with_context(|| format!("invalid tag regex: {pattern}"))?)
            } else {
                None
            };
            tags.push(CompiledTagRule {
                tag_type: rule.tag_type,
                includes: rule.includes,
                regex,
                tag: rule.tag,
            });
        }
        Ok(Arc::new(Self {
            blocklist: file.blocklist,
            formats: file.formats,
            tags,
        }))
    }

    pub fn is_blocked(&self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        self.blocklist.iter().any(|term| lower.contains(term))
    }

    pub fn detect_content_type(&self, names: &[String]) -> String {
        let mut weights: HashMap<&str, u32> = HashMap::new();
        for name in names {
            let Some(ext) = extension(name) else {
                continue;
            };
            for (kind, extensions) in &self.formats {
                if extensions.iter().any(|item| item == &ext) {
                    let key: &str = kind;
                    *weights.entry(key).or_insert(0) += 1;
                }
            }
        }
        weights
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(kind, _)| kind.to_string())
            .unwrap_or_default()
    }

    pub fn detect_tags(&self, names: &[String], content_type: &str) -> Vec<String> {
        let mut tags = Vec::new();
        for name in names {
            let lower = name.to_ascii_lowercase();
            for rule in &self.tags {
                if !tag_type_matches(&rule.tag_type, content_type) {
                    continue;
                }
                let matched = if let Some(needle) = &rule.includes {
                    lower.contains(needle)
                } else if let Some(re) = &rule.regex {
                    re.is_match(&lower)
                } else {
                    false
                };
                if matched && !tags.iter().any(|t| t == &rule.tag) {
                    tags.push(rule.tag.clone());
                }
            }
        }
        tags
    }
}

fn extension(name: &str) -> Option<String> {
    let segment = name.rsplit(['/', '\\']).next()?;
    let ext = segment.rsplit('.').next()?;
    if ext == segment {
        return None;
    }
    Some(ext.to_ascii_lowercase())
}

fn tag_type_matches(rule_type: &str, content_type: &str) -> bool {
    rule_type == content_type || (rule_type == "doc" && content_type == "document")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_classification_file() {
        let rules = ClassificationRules::load("config/classification.toml").unwrap();
        assert!(rules.is_blocked("hardcandy-pack"));
        assert!(!rules.is_blocked("ubuntu.iso"));
    }

    #[test]
    fn blocklist_matches_node_filter_count() {
        let content = std::fs::read_to_string("config/classification.toml").unwrap();
        let file: ClassificationFile = toml::from_str(&content).unwrap();
        assert!(
            file.blocklist.len() >= 140,
            "expected full blocklist port from Node filters.js"
        );
    }
}
