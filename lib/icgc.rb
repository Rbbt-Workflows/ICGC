require 'rbbt'
require 'net/ftp'

require 'rbbt/sources/ICGC'

module ICGC
  SERVER = 'data.dcc.icgc.org'
  VERSION='version_17'

  #WSERVER = 'dcc.icgc.org'
  #def self._ftp
  #  @ftp ||= begin
  #           ftp = Net::FTP.new(SERVER)
  #           ftp.login
  #           ftp.passive = true
  #           ftp
  #         end
  #end

  #def self._get_file(file)
  #  url = 'ftp://' << File.join(SERVER, file)
  #  Log.debug("Retrieving file: #{ url }")
  #  Open.open(url)
  #end

  #def self.get_file(file)
  #  file_url(file)
  #end

  ##new

  #def self.file_url(file)
  #  route = "api/v1/download?fn=" << file
  #  url = 'https://' << File.join(WSERVER, route)
  #  url
  #end

  #def self.open_file(file, &block)
  #  url = file_url(file)
  #  if block_given?
  #    Log.debug("Reading file: #{ url }")
  #    Open.open(url, &block)
  #  else
  #    Log.debug("Opening file: #{ url }")
  #    Open.open(url)
  #  end
  #end

  #def self.datasets
  #  open_file('/current/Projects/README.txt') do |f|
  #    f.read.split("\n").collect do |line|
  #      next unless line =~ /^-\s*([A-Z]+-[A-Z]+)\s.*-.*/
  #      $1
  #    end
  #  end.compact
  #end
  #
  #def self.dataset_file(dataset, file)
  #  basename = [file, dataset,'tsv.gz']*"."
  #  File.join('/current/Projects/', dataset, basename)
  #end

  #def self.open_dataset_file(dataset, file, &block)
  #  url = dataset_file(dataset, file)
  #  if block_given?
  #    open_file(url, &block)
  #  else
  #    open_file(url)
  #  end
  #end


  #FILE_TYPES = %w(copy_number_somatic_mutation donor donor_exposure donor_family donor_therapy exp_array exp_seq meth_array meth_seq sample simple_somatic_mutation simple_somatic_mutation.open specimen)
  #def self.dataset_files(dataset)
  #  return {} if dataset =~ /Readme/i
  #  Persist.persist("Dataset files", :yaml, :dir => Rbbt.tmp.ICGC, :other => {:dataset => dataset}) do
  #    files = {}
  #    FILE_TYPES.each do |file|
  #      basename = [file, dataset,'tsv.gz']*"."
  #      files[file] = File.join('/current/Projects/', dataset, basename)
  #    end
  #    files
  #  end
  #end
  ##old
  #
  #def self.dataset_files(dataset)
  #  return {} if dataset =~ /Readme/i
  #  Persist.persist("Dataset files", :yaml, :dir => Rbbt.tmp.ICGC, :other => {:dataset => dataset}) do
  #    sleep rand * 10
  #    ftp = self.ftp
  #    ftp.chdir(File.join('/' + VERSION, dataset))

  #    files = {}
  #    ftp.nlst.select{|n| n[0] != "!"}.each do |file|
  #      type, *rest = file.split '.'
  #      files[type] = File.join('/' + VERSION, dataset, file)
  #    end
  #    ftp.chdir(File.join('/' + VERSION, dataset))
  #    files
  #  end
  #end

  #FILE_TYPES = %w(copy_number_somatic_mutation donor donor_exposure donor_family donor_therapy exp_array exp_seq meth_array meth_seq sample simple_somatic_mutation simple_somatic_mutation.open specimen)
  #def self.dataset_files(dataset)
  #  return {} if dataset =~ /Readme/i
  #  Persist.persist("Dataset files", :yaml, :dir => Rbbt.tmp.ICGC, :other => {:dataset => dataset}) do
  #    files = {}
  #    FILE_TYPES.each do |file|
  #      basename = [file, dataset,'tsv.gz']*"."
  #      files[file] = File.join('/current/Projects/', dataset, basename)
  #    end
  #    files
  #  end
  #end

  #def self.dataset_organism(dataset)
  #  file = dataset_files(dataset)["simple_somatic_mutation"]
  #  if file.nil?
  #    Organism.default_code("Hsa")
  #  else
  #    begin
  #      tsv = TSV.open(CMD.cmd('head -n 3', :in => get_file(file), :pipe => true), :header_hash => "", :fields => ["assembly_version"], :type => :single)
  #      assembly = tsv.values.first
  #      case assembly
  #      when "GRCh37"
  #        "Hsa/dec2013"
  #      when "NCBI36"
  #        "Hsa/may2009"
  #      else
  #        raise "Assembly #{ assembly } not recognized"
  #      end
  #    rescue
  #      Log.warn $!.message
  #      Organism.default_code("Hsa")
  #    end
  #  end
  #end
end

