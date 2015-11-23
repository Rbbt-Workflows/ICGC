require 'rbbt'

module ICGC
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

  def self.datasets
    Open.open(download_url('/current/Projects/README.txt')) do |f|
      f.read.split("\n").collect do |line|
        next unless line =~ /^-\s*([A-Z]+-[A-Z]+)\s.*-.*/
          $1
      end
    end.compact
  end

end

