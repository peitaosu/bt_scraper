use crate::classification_config::ClassificationRules;
use crate::{TorrentFile, TorrentMetadata};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classification {
    pub content_type: String,
    pub tags: Vec<String>,
    pub blocked: bool,
}

pub fn classify(metadata: &TorrentMetadata, rules: &ClassificationRules) -> Classification {
    let names = file_names(metadata);
    let blocked = names.iter().any(|name| rules.is_blocked(name));
    let content_type = rules.detect_content_type(&names);
    let tags = rules.detect_tags(&names, &content_type);
    Classification {
        content_type,
        tags,
        blocked,
    }
}

fn file_names(metadata: &TorrentMetadata) -> Vec<String> {
    let mut names = Vec::new();
    names.push(metadata.name.clone());
    for file in &metadata.files {
        names.push(file_path_string(file));
    }
    names
}

pub fn flatten_file_paths(metadata: &TorrentMetadata) -> String {
    metadata
        .files
        .iter()
        .map(file_path_string)
        .collect::<Vec<_>>()
        .join(" ")
}

fn file_path_string(file: &TorrentFile) -> String {
    file.path.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classification_config::ClassificationRules;
    use std::sync::Arc;

    fn rules() -> Arc<ClassificationRules> {
        ClassificationRules::load("config/classification.toml").unwrap()
    }

    #[test]
    fn detects_video_type_from_extension() {
        let metadata = TorrentMetadata {
            name: "movie.mkv".into(),
            files: vec![TorrentFile {
                path: vec!["movie.mkv".into()],
                length: 1,
            }],
            total_length: 1,
        };
        let result = classify(&metadata, &rules());
        assert_eq!(result.content_type, "video");
        assert!(!result.blocked);
    }

    #[test]
    fn flags_blocklisted_names() {
        let rules = rules();
        assert!(rules.is_blocked("something-hardcandy-pack"));
        assert!(!rules.is_blocked("ubuntu-22.04.iso"));
    }

    #[test]
    fn flags_blocklisted_file_paths() {
        let rules = rules();
        let metadata = TorrentMetadata {
            name: "clean-title".into(),
            files: vec![TorrentFile {
                path: vec!["folder".into(), "hardcandy-pack.zip".into()],
                length: 1,
            }],
            total_length: 1,
        };
        let result = classify(&metadata, &rules);
        assert!(result.blocked);
    }

    #[test]
    fn extracts_show_tag_from_season_pattern() {
        let metadata = TorrentMetadata {
            name: "Show S01E02".into(),
            files: vec![TorrentFile {
                path: vec!["Show S01E02.mkv".into()],
                length: 1,
            }],
            total_length: 1,
        };
        let result = classify(&metadata, &rules());
        assert_eq!(result.content_type, "video");
        assert!(result.tags.iter().any(|t| t == "show"));
    }
}
