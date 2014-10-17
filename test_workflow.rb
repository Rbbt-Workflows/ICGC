require 'test/unit'
require 'rbbt/workflow'

Workflow.require_workflow "ICGC"

class TestICGC < Test::Unit::TestCase
  def test_true
    dataset = "Kidney_Renal_Papillary_Cell_Carcinoma-TCGA-US"
    ICGC[dataset].produce(true)
    iii ICGC[dataset].genotypes.glob("*")
  end
end

