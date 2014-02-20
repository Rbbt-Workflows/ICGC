require 'rbbt'
require 'net/ftp'

module ICGC
  SERVER = 'data.dcc.icgc.org'

  def self.ftp
    @ftp ||= begin
             ftp = Net::FTP.new(SERVER)
             ftp.login
             ftp.passive = true
             ftp
           end
  end

  def self.datasets
    ftp = self.ftp
    ftp.chdir('current')
    ftp.nlst.select{|n| n[0] != "!" and n != /Readme/i}
  end

  def self.dataset_files(dataset)
    return {} if dataset =~ /Readme/i
    Persist.persist("Dataset files", :yaml, :dir => Rbbt.tmp.ICGC, :other => {:dataset => dataset}) do
      sleep rand * 10
      ftp = self.ftp
      ftp.chdir(File.join('/current', dataset))

      files = {}
      ftp.nlst.select{|n| n[0] != "!"}.each do |file|
        type, *rest = file.split '.'
        files[type] = File.join('/current', dataset, file)
      end
      ftp.chdir(File.join('/current', dataset))
      files
    end
  end

  def self.get_file(file)
    url = 'ftp://' << File.join(SERVER, file)
    Log.debug("Retrieving file: #{ url }")
    Open.open(url)
  end

  def self.dataset_organism(dataset)
    file = dataset_files(dataset)["simple_somatic_mutation"]
    if file.nil?
      "Hsa/jan2013"
    else
      tsv = TSV.open(CMD.cmd('head -n 3', :in => get_file(file), :pipe => true), :header_hash => "", :fields => ["assembly_version"], :type => :single)
      assembly = tsv.values.first
      case assembly
      when "GRCh37"
        "Hsa/jan2013"
      when "NCBI36"
        "Hsa/may2009"
      else
        raise "Assembly #{ assembly } not recognized"
      end
    end
  end
end

