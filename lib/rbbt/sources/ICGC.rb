require 'rbbt'
require 'rbbt/sources/ICGC/format'

module ICGC
  extend Resource

  self.subdir = "share/data/projects/ICGC/"

  ICGC.claim ICGC.root, :rake, Rbbt.share.install.ICGC.Rakefile.find

  WSERVER = 'dcc.icgc.org'

  def self.download_url(file)
    route = "api/v1/download?fn=" << file
    url = 'https://' << File.join(WSERVER, route)
    url
  end

  def self.dataset_file(dataset, type)
    basename = [type, dataset,'tsv.gz']*"."
    File.join('/current/Projects/', dataset, basename)
  end

  def self.dataset_url(dataset, type)
    download_url dataset_file(dataset, type)
  end

  def self.path_info(path)
    JSON.parse(Open.read(File.join('https://dcc.icgc.org/api/v1/download/info/', path)))
  end

  def self.datasets(base_dir="/current/Projects")
    path_info(base_dir).collect do |info|
      next if info["name"].include? "README"
      File.basename(info["name"])
    end.compact
  end

  def self.dataset_files(dataset)
    path_info(File.join('/current/Projects/', dataset)).collect{|info| File.basename(info["name"]).split("\.#{dataset}\.").first }
  end
end

