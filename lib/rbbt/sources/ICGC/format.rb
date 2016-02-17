module ICGC
  module Format

    SAMPLE_FIELDS = ["submitted_specimen_id", "submitted_sample_id", "icgc_specimen_id", "icgc_donor_id"]
    EXPRESSION_FIELDS = ["normalized_expression_level", "normalized_expression_value", "normalized_read_count", "raw_read_count"]

    def self.organism
      Organism.default_code("Hsa")
    end


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

    def self.ICGC_tsv(url, fields, options = {}, &block)
      options = Misc.add_defaults options, :namespace => ICGC::Format.organism, :type => :list, :merge => true
      all_fields = TSV.parse_header(url, :header_hash => '').fields
      target_fields = options[:fields] || fields

      sample_pos, sample_field = ICGC::Format.find_field(all_fields, SAMPLE_FIELDS)

      dumper_options = {:key_field => sample_field, :fields => fields}.merge(options)
      dumper = TSV::Dumper.new(dumper_options)
      dumper.init
      #TSV.traverse url, :into => dumper, :key_field => sample_field, :fields => fields, :header_hash => '', :bar => "Processing #{ File.basename(url) }" do |sample,values|
      TSV.traverse url, :into => dumper, :key_field => sample_field, :fields => fields, :header_hash => '', :bar => "Processing #{ File.basename(url) }" do |sample,values|
        sample = sample.first if Array === sample

        block.call(sample, values)
      end

      if options[:merge]
        TSV.collapse_stream(dumper.stream)
      else
        dumper.stream
      end
    end

    #def self.ICGC_matrix(url, fields)
    #  organism = self.organism

    #  associations = ICGC_tsv(url, fields, :merge => false, :type => :single) do |sample,values|
    #    gene, value = values
    #    gene = gene.first if Array === gene
    #    gene = gene.split(".").first
    #    key = [sample,gene] * "~"
    #    [key, value]
    #  end

    #  matrix = {}
    #  TSV.traverse associations, :bar => "Associations to Matrix" do |item,value|
    #    sample, _sep, gene = item.partition("~")
    #    matrix[gene] ||= {}
    #    matrix[gene][sample] ||= []
    #    matrix[gene][sample] << value
    #  end

    #  samples = matrix[matrix.keys.first].keys.sort
    #  genes = matrix.keys.sort
    #  format, count = Organism.guess_id(organism, genes)

    #  dumper = TSV::Dumper.new :key_field => format, :fields => samples, :type => :double, :cast => :to_f, :namespace => organism
    #  dumper.init
    #  TSV.traverse matrix, :into => dumper, :bar => "Matrix to TSV" do |gene, data|
    #    sample_values = data.chunked_values_at(samples)
    #    [gene, sample_values]
    #  end
    #end

    def self.ICGC_matrix(url, fields, type = :double)
      organism = self.organism

      associations = ICGC_tsv(url, fields, :merge => false, :type => :single) do |sample,values|
        gene, value = values
        gene = gene.first if Array === gene
        gene = gene.split(".").first
        key = [sample,gene] * "~"
        [key, value]
      end

      raise "Type must be :list or :double but is #{type.inspect}" unless type == :double or type == :list

      index_file = TmpFile.tmp_file
      matrix = Persist.open_tokyocabinet(index_file, true, type == :list ? :float_array : type)
      samples =  []

      TSV.traverse associations, :bar => "Associations to Matrix" do |item,value|
        sample, _sep, gene = item.partition("~")

        samples << sample unless samples.include? sample

        sample_pos = samples.index sample

        v = matrix[gene] 
        v = Array.new{[]} if v.nil? and type == :double
        v = Array.new{-999} if v.nil?

        if type == :double
          v[sample_pos] = [] if v[sample_pos].nil?
          v[sample_pos] << value
          v.collect!{|e| e.nil? ? [] : e }
        else
          v[sample_pos] = value.to_f
        end

        v.collect!{|_v| _v.nil? ? -999 : _v}
        matrix[gene] = v
      end

      genes = matrix.keys.sort
      format, count = Organism.guess_id(organism, genes)

      matrix.read

      dumper = TSV::Dumper.new :key_field => format, :fields => samples, :type => type, :cast => :to_f, :namespace => organism
      dumper.init
      TSV.traverse matrix, :into => dumper, :bar => "Matrix to TSV" do |gene, sample_values|
        sample_values.collect!{|_v| _v == -999 ? nil : _v}
        [gene, sample_values]
      end

      stream = dumper.stream
      stream.add_callback do
        FileUtils.rm index_file
      end

      stream
    end

    def self.expression_array(url)
      fields = ["gene_id","normalized_expression_value"]
      ICGC_matrix(url, fields)
    end

    def self.expression_sequence(url)
      fields = ["gene_id","normalized_read_count"]
      ICGC_matrix(url, fields)
    end

    def self.protein_expression(url)
      fields = ["antibody_id","normalized_expression_level"]
      ICGC_matrix(url, fields)
    end

    def self.methylation_array(url)
      fields = ["probe_id","methylation_value"]
      ICGC_matrix(url, fields, :list)
    end

    def self.methylation_seq(url)
      fields = ["probe_id","methylation_value"]
      ICGC_matrix(url, fields)
    end

    def self.simple_somatic_mutation(url)
      fields = ["chromosome","chromosome_start", 'mutated_from_allele', 'mutated_to_allele']

      return ICGC_tsv(url, fields, :type => :flat, :fields => ["Genomic Mutation"]) do |sample,values|
        mutations = []
        Misc.zip_fields(values).each do |chr, pos, ref, mut|
          next if mut.nil?
          pos, muts = Misc.correct_icgc_mutation(pos.to_i, ref, mut)
          muts.each do |m|
            m.strip
            mutations << [chr, pos, m] * ":"
          end
        end
        [sample, mutations]
      end
    end

    def self.copy_number_somatic_mutation(url)
      fields = ["chromosome","chromosome_start", 'chromosome_end', 'mutation_type', 'segment_mean']

      return ICGC_tsv(url, fields, :type => :flat, :fields => ["CNV"]) do |sample,values|
        cnvs = []
        Misc.zip_fields(values).each do |chr, start, eend , mutation_type, mean|
           chr = chr.sub('chr','')
           if mean and not mean.empty?
             mean = mean.to_f
             cnv = ((2**mean) * 2)
             next unless cnv >= 3 or cnv <= 1
             cnvs << [chr,start,eend,"%.1f" % cnv] * ":"
           elsif mutation_type and not mutation_type.empty?
             cnvs << [chr,start,eend,mutation_type] * ":"
           else
             raise "No CNV information"
           end
        end
        [sample, cnvs]
      end
    end

  end
end
