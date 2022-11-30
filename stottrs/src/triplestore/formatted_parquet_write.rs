use super::Triplestore;

impl Triplestore {
    pub fn write_formatted_parquet(&mut self) {
        self.deduplicate();
        let non_string_props = self.get_non_string_property_triples();
        let string_props = self.get_string_property_triples();
        let object_props = self.get_object_property_triples();
    }
}