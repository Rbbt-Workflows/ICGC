module ICGC
  module Format

    SAMPLE_FIELDS = ["submitted_specimen_id", "submitted_sample_id", "icgc_specimen_id", "icgc_donor_id"]
    EXPRESSION_FIELDS = ["normalized_expression_level", "normalized_expression_value", "normalized_read_count", "raw_read_count"]


    def self.find_field(headers, probe_field)
      if Array === probe_field
        Log.debug("Trying fields: #{ probe_field.inspect }")
        probe_field.each do |pf|
          begin
            return find_field(headers, pf)
          rescue
            next
          end
        end
        raise "Fields #{probe_field.inspect} not identified. Options: #{headers * ", "}"
      else
        begin
          res = [Misc.field_position(headers, probe_field), probe_field]
          Log.debug("Found #{ probe_field }")
          res
        rescue Exception
          raise "Field '#{probe_field}' not identified. Options: #{headers * ", "}"
        end
      end
    end


    def self.somatic_mutations(url)
      type = 'simple_somatic_mutation.open'
      fields = TSV.parse_header(url, :header_hash => '').fields

      sample_pos, sample_field = ICGC::Format.find_field(fields, SAMPLE_FIELDS )

      mutation_field = 'mutated_to_allele'
      url = ICGC.dataset_url(dataset, type)
      tsv = TSV.open(url, 
                     :header_hash => '', :merge => true,
                     :key_field => sample_field, 
                     :unnamed => true,
                     :fields => ["chromosome","chromosome_start", 'mutated_from_allele', 'mutated_to_allele'])

      genotypes = TSV.setup({}, :key_field => sample_field, :fields => ["Genomic Mutation"], :type => :flat)

      tsv.through do |sample, values|
        genotypes[sample] = []

        Misc.zip_fields(values).each do |chr, pos, ref, mut|
          next if mut.nil?
          pos, muts = Misc.correct_icgc_mutation(pos.to_i, ref, mut)
          muts.each do |m|
            m.strip
            genotypes[sample] << [chr, pos, m] * ":"
          end
        end
      end

      genotypes.each{|sample, muts| muts.uniq! }

      genotypes.dumper
    end
  end
end
