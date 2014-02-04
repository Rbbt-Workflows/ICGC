require 'icgc'
require 'rbbt/workflow'
require 'rbbt/sources/organism'
require 'rbbt/tsv/change_id'

module ICGC
  extend Workflow

  SAMPLE_FIELDS = ["submitted_sample_id", "submitted_specimen_id"]#, "submitted_specimen_id", "submitted_donor_id"]
  EXPRESSION_FIELDS = ["normalized_expression_level", "normalized_read_count", "raw_read_count"]

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
      rescue
        raise "Field '#{probe_field}' not identified. Options: #{headers * ", "}"
      end
    end
  end

  def self.process_ICGC_matrix(fsource, probe_field, sample_field, value_field, output)
    headers = fsource.gets.split("\t")
    pos_probe, probe_field  = find_field headers, probe_field
    pos_sample, sample_field = find_field headers, sample_field
    pos_value, value_field  = find_field headers, value_field

    sample_positions = {}

    fsource = CMD.cmd("grep -v sequencing | sort -k #{pos_probe + 2}", :in => fsource, :pipe => true)

    TmpFile.with_file do |tmp|
      ftmp = Open.open(tmp, :mode => 'w')
      last_probe = nil
      values = []
      while line = fsource.gets do
        probe, sample, value = line.split("\t").values_at pos_probe, pos_sample, pos_value

        probe.sub!(/^Ensembl:/,'')

        next if probe.empty? or probe == "?"

        last_probe = probe if last_probe.nil?

        if probe != last_probe
          values.unshift last_probe
          ftmp.puts values * "\t"
          values = [nil] * sample_positions.size
          last_probe = probe
        end

        sample_position = sample_positions[sample] ||= sample_positions.size 
        values[sample_position] = value 
      end
      values.unshift probe
      ftmp.puts values * "\t"
      values = [nil] * sample_positions.size
      last_probe = probe
      fields = sample_positions.sort_by{|s,position| position }.collect{|sample,p| sample }
      fields.unshift "Ensembl Gene ID"

      Open.write(output, "#" + fields * "\t" + "\n")
      `cat "#{tmp}" >> "#{ output }"`
    end

    nil
  end

  def self.process_TCGA_matrix(fsource, probe_field, sample_field, value_field, output)
    headers = fsource.gets.split("\t")
    pos_probe, probe_field  = find_field headers, probe_field
    pos_sample, sample_field = find_field headers, sample_field
    pos_value, value_field  = find_field headers, value_field
    pos_platform, platform_field  = find_field headers, "Platform"

    sample_positions = {}

    fsource = CMD.cmd("sort -k #{pos_probe + 2}", :in => fsource, :pipe => true)

    translate_mistakes_in_source = Organism.identifiers("Hsa/jan2013").index(:target => "Ensembl Gene ID", :persist => true, :data_persist => true)

    TmpFile.with_file do |tmp|
      ftmp = Open.open(tmp, :mode => 'w')
      last_probe = nil
      good_platform = nil
      values = []
      while line = fsource.gets do
        probe, sample, value, platform = line.split("\t").values_at pos_probe, pos_sample, pos_value, pos_platform

        next if probe.empty? or probe == "?"

        good_platform ||= platform unless probe.empty? or value.empty?
        next if platform != good_platform

        last_probe = probe if last_probe.nil?

        if probe != last_probe
          values.unshift translate_mistakes_in_source[last_probe]
          ftmp.puts values * "\t"
          values = [nil] * sample_positions.size
          last_probe = probe
        end

        sample_position = sample_positions[sample] ||= sample_positions.size 
        values[sample_position] = value 
      end
      values.unshift translate_mistakes_in_source[probe]
      ftmp.puts values * "\t"
      values = [nil] * sample_positions.size
      last_probe = probe
      fields = sample_positions.sort_by{|s,position| position }.collect{|sample,p| sample }
      fields.unshift probe_field

      Open.write(output, "#: :description=" + value_field + "#:platform=" + good_platform + "\n")
      Open.write(output, "#" + fields * "\t" + "\n", :mode => 'a')
      `cat "#{tmp}" >> "#{ output }"`
    end

    nil
  end


  task :datasets => :tsv do
    tsv = TSV.setup({}, 
                    :key_field => "Study", 
                    :fields => ["Clinical", "Genomic Mutation", "CNV", "Gene Expression", "Methylation"],
                    :type => :list
                   )

    ICGC.datasets.each do |dataset|
      dataset_info = ICGC.dataset_files(dataset)
      tsv[dataset] = %w(clinical simple_somatic_mutation cnv gene_expression methylation).collect{|type|
        dataset_info.include? type
      }
    end
    tsv
  end

  input :dataset, :string, "ICGC dataset code"
  task :get_clinicalsample => :tsv do |dataset|
    fields = Open.open(ICGC.get_file(ICGC.dataset_files(dataset)['clinicalsample'])) do |stream|
      TSV.parse_header(stream, :header_hash => '').fields
    end

    sample_pos, sample_field = ICGC.find_field fields, SAMPLE_FIELDS

    tsv = TSV.open(ICGC.get_file(ICGC.dataset_files(dataset)['clinicalsample']), :header_hash => '', :type => :list, :key_field => sample_field)

    tsv
  end


  input :dataset, :string, "ICGC dataset code"
  task :get_clinical => :tsv do |dataset|
    fields = Open.open(ICGC.get_file(ICGC.dataset_files(dataset)['clinical'])) do |stream|
      TSV.parse_header(stream, :header_hash => '').fields
    end
    sample_pos, sample_field = ICGC.find_field fields, SAMPLE_FIELDS

    tsv = TSV.open(ICGC.get_file(ICGC.dataset_files(dataset)['clinical']), :header_hash => '', :type => :list, :key_field => sample_field)

    tsv
  end

  input :dataset, :string, "ICGC dataset code"
  task :get_simple_somatic_mutation => :tsv do |dataset|
    fields = Open.open(ICGC.get_file(ICGC.dataset_files(dataset)['simple_somatic_mutation'])) do |stream|
      TSV.parse_header(stream, :header_hash => '').fields
    end
    sample_pos, sample_field = ICGC.find_field fields, SAMPLE_FIELDS #["Specimen ID", "Donor ID", "Analyzed sample ID", "Sample ID"]

    mutation_field = 'tumour_genotype'
    tsv = TSV.open(ICGC.get_file(ICGC.dataset_files(dataset)['simple_somatic_mutation']), 
                   :header_hash => '', :merge => true,
                   :key_field => sample_field, 
                   :fields => ["chromosome","chromosome_start", mutation_field]
                  )

    genotypes = TSV.setup({}, :key_field => "Sample", :fields => ["Genomic Mutation"], :type => :flat)

    tsv.through do |sample, values|
      genotypes[sample] = []
      values.zip_fields.each do |chr, pos, mut|
        next if mut.nil?
        ref, mut = mut.split(/>|\//)
        pos, muts = Misc.correct_icgc_mutation(pos.to_i, ref, mut)
        muts.each do |m|
          m.strip
          genotypes[sample] << [chr, pos, m] * ":"
        end
      end
    end

    genotypes.each{|sample, muts| muts.uniq! }

    genotypes
  end

  input :dataset, :string, "ICGC dataset code"
  task :get_cnv => :tsv do |dataset|
    fields = Open.open(ICGC.get_file(ICGC.dataset_files(dataset)['cnv'])) do |stream|
      TSV.parse_header(stream, :header_hash => '').fields
    end
    sample_pos, sample_field = ICGC.find_field fields, SAMPLE_FIELDS #["Specimen ID", "Donor ID", "Analyzed sample ID", "Sample ID"]
    mutation_pos, mutation_field = ICGC.find_field fields, ["Segment mean", "Mutation type"]

    tsv = TSV.open(ICGC.get_file(ICGC.dataset_files(dataset)['cnv']), 
                   :header_hash => '', :merge => true,
                   :key_field => sample_field, 
                   :fields => ["Chromosome","Chromosome start", "Chromosome end", mutation_field]
                  )

    cnvs = TSV.setup({}, :key_field => "Sample", :fields => ["CNV"], :type => :flat)

    tsv.through do |sample, values|
      cnvs[sample] = []
      values.zip_fields.uniq.each do |chr, start, eend, mut|
        next if mut.to_f.abs < 1 if mutation_field == "Segment mean"
        cnvs[sample] << [chr, start, eend, mut] * ":"
      end
    end

    cnvs
  end

  input :dataset, :string, "ICGC dataset code"
  task :get_gene_expression => :tsv do |dataset|
    fsource = ICGC.get_file(ICGC.dataset_files(dataset)['gene_expression'])
    if dataset =~ /TCGA/
      ICGC.process_TCGA_matrix(fsource, "gene_stable_id", SAMPLE_FIELDS, EXPRESSION_FIELDS, path)
    else                                
      ICGC.process_ICGC_matrix(fsource, "gene_stable_id", SAMPLE_FIELDS, EXPRESSION_FIELDS, path)
    end
  end

  input :dataset, :string, "ICGC dataset code"
  task :get_methylation => :tsv do |dataset|
    fsource = ICGC.get_file(ICGC.dataset_files(dataset)['methylation'])
    ICGC.process_ICGC_matrix(fsource, "Methylated fragment ID", "Analyzed sample ID", "Methylation Beta value", path)
  end

  export_asynchronous :datasets, :get_clinical, :get_simple_somatic_mutation, :get_cnv, :get_gene_expression, :get_methylation

  input :dataset, :string, "ICGC dataset code"
  input :output, :string, "Path where to create study"
  def self.prepare_study(dataset, output)
    FileUtils.mkdir_p output unless File.exists? output
    organism = dataset_organism(dataset)
    Open.write(File.join(output, 'metadata.yaml'), {:condition => Misc.humanize(dataset.sub(/-.*/,'')), :organism => organism, :watson => true}.to_yaml)
    files = dataset_files(dataset)

    if files.include? "clinicalsample"
      FileUtils.cp(ICGC.job(:get_clinicalsample, dataset, :dataset => dataset).run(true).path, File.join(output, 'identifiers'))
    end
 
    if files.include? "clinical"
      sample_info = ICGC.job(:get_clinical, dataset, :dataset => dataset).run(true).path.tsv :type => :double
      sample_info.identifiers = File.join(output, 'identifiers')
      sample_info = sample_info.change_key "submitted_sample_id"
      sample_info.key_field = "Sample ID"
      Open.write(File.join(output, 'samples'), sample_info.to_s)
    end

    if files.include? "simple_somatic_mutation"
      ICGC.job(:get_simple_somatic_mutation, dataset, :dataset => dataset).run.each do |sample, genotype|
        Open.write(File.join(output, 'genotypes', sample), genotype * "\n")
      end
    end
 
    if files.include? "cnv"
      ICGC.job(:get_cnv, dataset, :dataset => dataset).run.each do |sample, genotype|
        Open.write(File.join(output, 'cnv', sample), genotype * "\n")
      end
    end
 
    if files.include? "gene_expression"
      FileUtils.mkdir_p File.join(output, 'matrices/gene_expression') unless File.exists? File.join(output, 'matrices/gene_expression')
      FileUtils.cp(ICGC.job(:get_gene_expression, dataset, :dataset => dataset).run(true).path, File.join(output, 'matrices/gene_expression', 'data'))
    end

    nil
  end
  task :prepare_study => nil

  extend Resource
  self.subdir = "share/studies/ICGC/"
  ICGC.claim ICGC.root, :rake, Rbbt.share.install.ICGC.Rakefile.find
end
