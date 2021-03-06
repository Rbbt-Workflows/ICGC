module ICGC


  FILE_TYPES = {
    "specimen"=> "Specimen",
    "sample"=> "Sample",
    "donor"=> "Donor",
    "donor_exposure"=> "Donor exposure",
    "donor_family"=> "Donor family",
    "donor_therapy"=> "Donor therapy",

    "simple_somatic_mutation.open"=> "Somatic SNVs",
    "copy_number_somatic_mutation"=> "Somatic CNVs",

    "exp_array"=> "Gene expression array",
    "exp_seq"=> "Gene expression RNAseq",
    "protein_expression"=> "RPPA",
    "meth_array"=> "Methylation array",

    "splice_variant"=> "Splice variants",
    "structural_somatic_mutation" => "Somatic SVs",
    "meth_seq"=> "Methylation sequencing",
    "mirna_seq"=> "MiRNA expression RNAseq",
  }

  SAMPLE_INFO_FILES = %w(specimen sample donor donor_exposure donor_family donor_therapy)

  task :datasets => :array

  input :dataset, :string, "Dataset code"
  task :dataset_files => :array 

  input :dataset, :string, "Dataset code"
  input :file, :select, "Dataset file", nil, :select_options => FILE_TYPES.keys
  task :dataset_url => :string 

  dep :datasets
  task :dataset_overview => :tsv do
    tsv = TSV.setup({}, 
                    :key_field => "Study", 
                    :fields => FILE_TYPES.values,
                    :type => :list)

    TSV.traverse step(:datasets), :bar => true, :type => :array do |dataset|
      dataset_info = ICGC.dataset_files(dataset)
      tsv[dataset] = FILE_TYPES.keys.collect{|type|
        dataset_info.include? type
      }
    end

    tsv
  end
  export_asynchronous :dataset_overview

  dep :dataset_url do |jobname,options|
    options[:dataset] = jobname if jobname and options[:dataset].nil?
    ICGC.job(:dataset_url, nil, options)
  end 
  task :get_file => :tsv do |file|
    file = step(:dataset_url).inputs[:file]
    url = step(:dataset_url).load

    case file
    when 'simple_somatic_mutation.open'
      ICGC::Format.simple_somatic_mutation(url)
    when 'copy_number_somatic_mutation'
      ICGC::Format.copy_number_somatic_mutation(url)
    when 'exp_array'
      ICGC::Format.expression_array(url)
    when 'exp_seq'
      ICGC::Format.expression_sequence(url)
    when 'protein_expression'
      ICGC::Format.protein_expression(url)
    when 'meth_array'
      ICGC::Format.methylation_array(url)
    else
      io = Open.open(url, {:wget_options => {"--no-check-certificate" => true}})
      Misc.open_pipe do |pin|
        pin.write("#")
        Misc.consume_stream io, false, pin
        io.join if io.respond_to? :join
      end
    end
  end


  input :dataset, :string, "ICGC dataset code"
  input :output, :string, "Path where to create study"
  def self.prepare_study(dataset, output)
    FileUtils.mkdir_p output unless File.exists? output
    organism = ICGC::Format.organism
    Open.write(File.join(output, 'metadata.yaml'), {:condition => Misc.humanize(dataset.sub(/-.*/,'')), :organism => organism, :watson => true}.to_yaml)

    files = dataset_files(dataset)

    sample_info = ICGC.job(:get_file, dataset, :file => 'sample').run 
    Open.write(File.join(output, 'samples'), sample_info.reorder("submitted_sample_id",nil, :zipped => true).to_s)

    SAMPLE_INFO_FILES.each do |file|
      next if file == 'sample'
      next unless files.include? file
      FileUtils.cp(ICGC.job(:get_file, dataset, :file => file).produce.path, File.join(output, file))
    end

    if files.include? "simple_somatic_mutation.open"
      ICGC.job(:get_file, dataset, :file => 'simple_somatic_mutation.open').run.each do |sample, genotype|
        Open.write(File.join(output, 'genotypes', sample), genotype * "\n")
      end
    end

    if files.include? "copy_number_somatic_mutation"
      ICGC.job(:get_file, dataset, :file => 'copy_number_somatic_mutation').run.each do |sample, genotype|
        Open.write(File.join(output, 'CNV', sample), genotype * "\n")
      end
    end

    if files.include? "exp_array"
      FileUtils.mkdir_p File.join(output, 'matrices','exp_array') unless File.exists? File.join(output, 'matrices','exp_array')
      FileUtils.cp(ICGC.job(:get_file, dataset, :file => 'exp_array').produce.path, File.join(output, 'matrices', 'exp_array','data'))
    end

    if files.include? "exp_seq"
      FileUtils.mkdir_p File.join(output, 'matrices','exp_seq') unless File.exists? File.join(output, 'matrices','exp_seq')
      FileUtils.cp(ICGC.job(:get_file, dataset, :file => 'exp_seq').produce.path, File.join(output, 'matrices', 'exp_seq','data'))
    end

    if files.include? "protein_expression"
      FileUtils.mkdir_p File.join(output, 'matrices','protein_expression') unless File.exists? File.join(output, 'matrices','protein_expression')
      FileUtils.cp(ICGC.job(:get_file, dataset, :file => 'protein_expression').produce.path, File.join(output, 'matrices', 'protein_expression','data'))
    end

    "done"
  end
  task :prepare_study => :string 

end
