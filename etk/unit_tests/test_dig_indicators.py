# -*- coding: utf-8 -*-
import unittest
import sys, os

sys.path.append('../../')
from etk.core import Core
import json
import codecs

class TestDigIndicators(unittest.TestCase):
    def setUp(self):
        fmp = '/Users/amandeep/Github/dig3-resources/indicator_data/fasttext_vec20_model.bin'
        imp = '/Users/amandeep/Github/dig3-resources/indicator_data/incall_model.sav'
        omp = '/Users/amandeep/Github/dig3-resources/indicator_data/outcall_model.sav'
        momp = '/Users/amandeep/Github/dig3-resources/indicator_data/movement_model.sav'
        mump = '/Users/amandeep/Github/dig3-resources/indicator_data/multi_model.sav'
        rmp = '/Users/amandeep/Github/dig3-resources/indicator_data/risky_model.sav'
        self.etk_config = {
            "extraction_policy": "replace",
            "document_id": "doc_id",
            "data_extraction": [
                {
                    "input_path": "test.text.`parent`",
                    "fields": {
                        "indicators": {
                            "extractors": {
                                "extract_dig_indicators": {
                                    "config": {
                                        "fasttext_model": fmp,
                                        "incall_model": imp,
                                        "outcall_model": omp,
                                        "movement_model": momp,
                                        "multi_model": mump,
                                        "risky_model": rmp
                                }
                                }
                            }

                        }
                    }
                }
            ]
        }

    def test_no_config(self):
        doc = {
            "doc_id": "123",
            'test': {
                'text': 'incall only guys'
            }
        }
        # c = Core(extraction_config=self.etk_config)
        # r = c.process(doc)
        # print json.dumps(r['knowledge_graph'], indent=2)
        # self.assertTrue(r)
        # self.assertTrue("content_extraction" not in r)
