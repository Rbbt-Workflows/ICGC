module ICGC


  FILE_TYPES = {
    "specimen"=> "Specimen",
    "sample"=> "Sample",
    "donor"=> "Donor",
    "donor_exposure"=> "Donor exposure",
    "donor_family"=> "Donor family",
    "donor_therapy"=> "Donor therapy",
    "splice_variant"=> "Splice variants",
    "simple_somatic_mutation.open"=> "Somatic SNVs",
    "copy_number_somatic_mutation.open"=> "Somatic CNVs",
    "structural_somatic_mutation" => "Somatic SVs",
    "exp_array"=> "Gene expression array",
    "exp_seq"=> "Gene expression RNAseq",
    "meth_array"=> "Methylation array",
    "meth_seq"=> "Methylation sequencing",
    "mirna_seq"=> "MiRNA expression RNAseq",
    "protein_expression"=> "RPPA",
  }

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

  dep :dataset_url
  task :get_file => :tsv do |file|
    file = step(:dataset_url).inputs[:file]
    url = step(:dataset_url).load

    case file
    when 'simple_somatic_mutation.open'
      ICGC::Format.somatic_mutations(url)
    else
      Open.open(url, :nocache => true)
    end
  end

end
