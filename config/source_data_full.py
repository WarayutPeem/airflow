def get_object_by_table_full(table_name):

    object_all_table = {

        # table_name : supplier
        "supplier": {
            "select": """
                supplierid, suppliercode, suppliername
            """,
            "from": "etl.view_supplier",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : product
        "product": {
            "select": """
                productid, productcode, productname, producttype, productstatus,
                saledatefrom, saledateto, planname, packagename, salebookcode,
                netamount, coveramount, amount, vat, duty, vatrate, dutyrate, 
                coverperiod, coverperiodunit, coverperiodunit_des, productfor, 
                supplierid, productgroup
            """,
            "from": "etl.view_product",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : promotion
        "promotion": {
            "select": """
                supplierid, promotioncode, promotioncodesupplier, promotionname, insurelevel, 
                insurelevel_des, cover, coverv3rd, coverv3rdtime, coverv3rdasset, 
                coverdeduct1, coverdeduct2, coverext, coveraccd, coveraccp, coverpassenger, 
                coveracc2d, coveracc2p, covermedd, coverlegal, premiummain, premiumendose,
                premiumadd, discountage, discountdeduct, discountfleet, discountother,
                discountexp, standardpremium, amount, duty, vat,
                promotionstatus, promotionstatus_des, carusage, cargroup, carbrand,
                carmodel, yearmin, yearmax, promotiontype, startdate, enddate
            """,
            "from": "etl.view_promotion",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : receivecost
        "receivecost": {
            "select": """           
                receivecostcode, receivecostname, chargeto, interest, installment,
                bankcode, bankname, receivecostgroup, receivecostgroup_des
            """,
            "from": "etl.view_receivecost",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : result
        "result": {
            "select": """
                resultid, resultcode, resultname, resulttype
            """,
            "from": "etl.view_result",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : staff
        "staff": {
            "select": """
                staffid, staffcode, staffname, stafftype, stafftype_des,
                staffstatus, staffstatus_des, departmentid
            """,
            "from": "etl.view_staff",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : department
        "department": {
            "select": """
                departmentid, departmentcode, departmentname, masterid, levelid, companyid, 
                departmentgroup, departmentgroupsub, departmentgroup_mod, departmentgroupsub_mod
            """,
            "from": "etl.view_department",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : departmenttree
        "departmenttree": {
            "select": """
                departmentid, departmentcode, departmentname, levelid,
                subdepartmentid, subdepartmentcode, subdepartmentname, sublevelid
            """,
            "from": "etl.view_departmenttree",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : batchcodeassigninfo
        "batchcodeassigninfo": {
            "select": """
                batchcode, batchgroup, assignmonth, caryear, carmonth, carmonth_type
            """,
            "from": "etl.view_batchcodeassigninfo",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : chatsurvey
        "chatsurvey": {
            "select": """
                chatsurveyid, surveyname, surveytype, surveystatus,
                title1, question1, title2, question2, title3, question3, title4, question4, title5, question5
            """,
            "from": "etl.view_chatsurvey",
            "field_action": {
                "field_encrypt": {}
            }
        },

        # table_name : sysbytedes
        "sysbytedes": {
            "select": """
                tablename, columnname, bytecode, bytedes
            """,
            "from": "etl.view_sysbytedes",
            "field_action": {
                "field_encrypt": {}
            }
        },

    }

    # get object with table name
    obj_table = object_all_table.get(table_name)

    return obj_table