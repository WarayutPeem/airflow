# B2S defualt conditipn
corp_prefix = ('บริษัท','ห้างหุ้นส่วนจำกัด','สหกรณ์','หจก.','เทศบาล','สำนักงาน','บจ.','ห้างหุ้นส่วน','นิติบุคคล','โรงเรียน',
                'CO.,LTD.','กรม','องค์การ','บจก.','นิติบุคคลอาคารชุด','มหาวิทยาลัย','มูลนิธิ','วัด','กองทัพบก','การไฟฟ้าส่วนภูมิภาค',
                'โรงพยาบาล','บมจ.','ร้าน','วิทยาลัย','สมาคม','ห้างหุ้นส่วนสามัญ','นิติบุคลอาคารชุด','ส่วนราชการ','คณะ','ศูนย์','กลุ่มบริษัท')


def get_object_table(table_name, from_date, to_date):

    object_all_table = {
###################################### INCREMENTAL TABLE ######################################
        # table_name : receiveitemclear
        "receiveitemclear": {
            "select": """
                saleid, installment, receiveid, receiveitem, clearamount, paiddate, cardtype, receiveamount, 
                cardnumber, receivedate, receivecostcode, bankcode, bankname, receiveitemstatus, 
                receiveitemstatus_des, receivebookcode, receivebookname, receivebookgroup
            """,
            "from": "etl.view_receiveitemclear",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "receiveitemclear",
                "field_name": ["saleid", "installment", "receiveid", "receiveitem"]
            }
        },

        # table_name : sale
        "sale": {
            "select": """
                saleid,staffid,leadid,leadassignid,periodid,salebookcode,sequence,productid,
                promotioncode,saletype,salestatus,salestatus_des,cancelresultid,canceldate,
                cancelremark,prbcancelresultid,prbcanceldate,prbcancelremark,policystatus,
                prbstatus,saledate,supplierdate,policynumber,policydate,expirydate,prbcode,
                prbsupplierid,prbcoverdate,prbexpirydate,paymentmode,paymentmodename,
                paymentstatus,balance,installment,cardtype,cardbankid,salepriority,amount,
                vat,duty,netamount,netvalue,prbamount,prbvat,prbduty,nominee,relationtext,
                insurelevel,insurelevel_des,carusage,carbrand,carmodel,caryear,carcc,carseat,
                carweight,carcolor,cargear,engineid,chasisid,bodytype,plateid,plateprovincename,
                caroptionvalue,policyaddresstype,contactaddresstype,postaddresstype,customeraddresstype,
                routeid,routecode,routegroup,corpstatus,insureprefix,insurename,insuresurname,
                citizenid,birthdate,gender,maritalstatus,child,jobname,jobtitle,income,jobcompany,
                phonemobile,phonehome,phoneoffice,email,customerprefix,customername,customersurname,
                customercitizenid,customerbirthdate,customergender,customermaritalstatus,
                customerjobname,customerjobtitle,customerincome,discountage,discountdeduct,
                discountfleet,discountother,discountexp,exdiscountsale,exdiscountcom,
                cover,coverv3rd,coverv3rdtime,coverv3rdasset,coverdeduct1,coverdeduct2,coverext,
                coveraccd,coveraccp,coverpassenger,coveracc2d,coveracc2p,covermedd,coverlegal,
                replacesaleid,extendsaleid,extendcount
            """,
            "from": "etl.view_sale",
            "where": f"""
                NVL(corpstatus, 'X') != 'Y'
                AND customerprefix NOT IN {corp_prefix}
                AND insureprefix NOT IN {corp_prefix}
                -------------------------
                AND changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"nominee", "chasisid", "plateid", "insurename", "insuresurname", "citizenid",
                                  "phonemobile", "phonehome", "phoneoffice", "customername", "customersurname",
                                  "customercitizenid", "policynumber", "email"}
            },
            "object_for_update": {
                "table_name": "sale",
                "field_name": ["saleid"]
            }
        },

        # table_name : saleaction
        "saleaction": {
            "select": """
                saleid, sequence, requestuserid, requestdate, actionuserid, actionid, 
                actioncode, actionname, actiondate, actionstatus, resultid, duedate
            """,
            "from": "etl.view_saleaction",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "saleaction",
                "field_name": ["saleid", "sequence"]
            }
        },

        # table_name : saledata
        "saledata": {
            "select": """            
                saleid, saleitem, datacode, valuetext, valuenumber, valuedate
            """,
            "from": "etl.view_saledata",
            "where": f"""         
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"valuetext"}
            },
            "object_for_update": {
                "table_name": "saledata",
                "field_name": ["saleid", "saleitem", "datacode"]
            }
        },

        # table_name : saleaddress
        "saleaddress": {
            "select": """
                saleid, addresstype, address1, address2, district, zipcode, provincecode, provincename
            """,
            "from": "etl.view_saleaddress",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"address1"}
            },
            "object_for_update": {
                "table_name": "saleaddress",
                "field_name": ["saleid", "addresstype"]
            }
        },

        # table_name : salepayment
        "salepayment": {
            "select": """
                saleid, installment, paymenttype, paymenttype_des, duedate, netamount, 
                balance, documentid, documentid_des, receivecostcode
            """,
            "from": "etl.view_salepayment",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {},
            },
            "object_for_update": {
                "table_name": "salepayment",
                "field_name": ["saleid", "installment"]
            }
        },

        # table_name : customer
        "customer": {
            "select": """
                customerid, customertype, identitykey, identitytype, customerprefix, 
                customername, customersurname, birthdate, customergender, customersegment 
            """,
            "from": "etl.view_customer",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"identitykey", "customername", "customersurname"},
            },
            "object_for_update": {
                "table_name": "customer",
                "field_name": ["customerid"]
            }
        },

        # table_name : customersale
        "customersale": {
            "select": """
                customerid, saleid, persontype, synctype, syncstatus, syncdate
            """,
            "from": "etl.view_customersale",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "customersale",
                "field_name": ["customerid", "saleid", "persontype"]
            }
        },

        # table_name : renewalnotice
        "renewalnotice": {
            "select": """
                fileid, sequence, filecode, documentno, documentdate, 
                newcover, newamount, newduty, newvat,
                newdiscountdeduct, newdiscountfleet, newdiscountfleetrate, newdiscountexp,
                newdiscountexprate, newdiscountother, newdiscountotherrate,
                claimrightno, claimwrongno, claimdeduct, claimdate, claimtype,
                saleid, updateable, createdatetime
            """,
            "from": "etl.view_renewalnotice",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "imptxt_renewalnotice_data",
                "field_name": ["fileid", "sequence"]
            }
        },

        # table_name : mapmembershipcust
        "mapmembershipcust": {
            "select": f"""
                membershipid, customerid, pairdate, ntier, last_updated
            """,
            "from": "etl.view_mapmembercust",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "map_membership_cust",
                "field_name": ["membershipid", "customerid"]
            }
        },

        # table_name : leadassign
        "leadassign": {
            "select": """
                leadid, leadassignid, batchcode, assigndate, startdate,
                enddate, assignstatus, assignactive, assignremark, staffremark,
                staffid, leadcarid, resultid, producttype, leadgroup, policyid, 
                premiumamount, saleid, leadclass
            """,
            "from": "etl.view_leadassign",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "leadassign",
                "field_name": ["leadid", "leadassignid"]
            }
        },

        # table_name : leadcar
        "leadcar": {
            "select": """
                leadid, leadcarid, plateid, engineid, chasisid,
                carbrand, carmodel, carcc, color, province,
                caryear, carmonth, registerdate, buydate, policydate, expirydate,
                relation, leadcarclass, createdatetime
            """,
            "from": "etl.view_leadcar",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"plateid", "chasisid"}
            },
            "object_for_update": {
                "table_name": "leadcar",
                "field_name": ["leadid", "leadcarid"]
            }
        },

        # table_name : lead
        "lead": {
            "select": """
                leadid, leadprefix, leadname, leadsurname, citizenid,
                birthdate, phonehome, phonemobile, email, createdatetime, gender, 
                jobname, jobtitle, homeaddress, officeaddress, contactaddress, leadmasterid
            """,
            "from": "etl.view_lead",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"leadname", "leadsurname", "citizenid", "phonehome", "phonemobile", "email",
                                  "homeaddress", "officeaddress", "contactaddress"}
            },
            "object_for_update": {
                "table_name": "lead",
                "field_name": ["leadid"]
            }
        },

        # table_name : leadaction
        "leadaction": {
            "select": """
                leadid, leadassignid, sequence, assigntype, assigntype_des, 
                statusdate, resultid, phoneno, talktime, actionstaffid
            """,
            "from": "etl.view_leadaction",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"phoneno"}
            },
            "object_for_update": {
                "table_name": "leadaction",
                "field_name": ["leadid", "leadassignid", "sequence"]
            }
        },

        # table_name : leaddata
        "leaddata": {
            "select": """
                leadid, leadassignid, datacode, valuetext, valuenumber, valuedate
            """,
            "from": "etl.view_leaddata",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"valuetext"}
            },
            "object_for_update": {
                "table_name": "leaddata",
                "field_name": ["leadid", "leadassignid", "datacode"]
            }
        },

        # table_name : leadtrack - have issue
        "leadtrack": {
            "select": """
                leadid, utmsource, utmmedium, utmcampaign, reference,
                saleid, order_id, leadassignid, promotionpageid, createdatetime
            """,
            "from": "etl.view_leadtrack",
            "where": f"""
                createdatetime >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND createdatetime < TO_DATE('{to_date}', 'YYYY-MM-DD')
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "",
                "field_name": []
            }
        },

        # table_name : leadchatclient
        "leadchatclient": {
            "select": """
                leadid, chatclientid, createdatetime
            """,
            "from": "etl.view_leadchatclient",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "leadchatclient",
                "field_name": ["leadid", "chatclientid"]
            }
        },

        # table_name : smsitem
        "smsitem": {
            "select": """
                smsbatchcode, sequence, smsbatchgroup, smsbatchdate, phoneno, 
                leadid, leadassignid, saleid, logsmsid, user_name, success, failed, 
                expired, logmessage, sender
            """,
            "from": "etl.view_smsitem",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"phoneno"}
            },
            "object_for_update": {
                "table_name": "smsitem",
                "field_name": ["smsbatchcode", "sequence"]
            }
        },

        # table_name : tqmappuser
        "tqmappuser": {
            "select": """
                id, tqm_user_id, created_at, phone_number, citizen_id,
                first_name, last_name, u_id, email, social_name,
                is_customer_verify, line_uid, facebook_uid, birthdate, gender,
                apple_uuid, customer_segment, consent_id, term_and_condition_id, last_active_at
            """,
            "from": "etl.view_tqmappuser",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"phone_number", "citizen_id", "first_name", "last_name", "email", "line_uid",
                                  "facebook_uid", "apple_uuid"}
            },
            "object_for_update": {
                "table_name": "users",
                "field_name": ["id"]
            }
        },

        # table_name : tqmappnoti
        "tqmappnoti": {
            "select": """
                id, title, schedule_time, user_id, is_read, is_view
            """,
            "from": "etl.view_tqmappnoti",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "notification_schedules",
                "field_name": ["id"]
            }
        },

        # table_name : web30tempsale
        "web30tempsale": {
            "select": f"""
                id, saledate, tqmsale_leadid, tqmsale_leadassignid, producttype, productplan,  
                utm_source, utm_medium, utm_campaign, channel
            """,
            "from": "etl.view_web30tempsale",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "impweb30_tempsale",
                "field_name": ["id"]
            }
        },

        # table_name : chatcenter
        "chatcenter": {
            "select": f"""
                chatclientid,chatassignid,chatroomid,assignstatus,
                leadid,leadassignid,leadactionsequence,lastchatclientsequence,
                lastchatstaffsequence,lastchatclientdatetime,lastchatstaffdatetime,
                lastchatstatus,saleid,userid,usertype,userstatus,
                roomname,roomchannel,referencecode,createdatetime
            """,
            "from": "etl.view_chatcenter",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"userid"}
            },
            "object_for_update": {
                "table_name": "chatcenter",
                "field_name": ["chatclientid", "chatassignid", "chatroomid"]
            }
        },

        # table_name : lineitem
        "lineitem": {
            "select": f"""
                linebatchcode, batchsequence, linebatchdate, phoneno, lineuserid,
                submitstatus, submitdatetime, leadid, leadassignid, sequence, saleid
            """,
            "from": "etl.view_lineitem",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"lineuserid", "phoneno"}
            },
            "object_for_update": {
                "table_name": "linebatchcode",
                "field_name": ["linebatchcode", "batchsequence"]
            }
        },

        # table_name : chatsurveyanswer
        "chatsurveyanswer": {
            "select": """
                chatroomid, chatclientid, chatassignid, chatsurveyid,
                answer1, answer2, answer3, answer4, answer5,
                remark, senddatetime, sendstaffid, submitdatetime
            """,
            "from": "etl.view_chatsurveyanswer",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "chatsurveyanswer",
                "field_name": ["chatroomid", "chatclientid", "chatassignid", "chatsurveyid"]
            }
        },

        # table_name : membership
        "membership": {
            "select": """
                membershipid, membershipcode, membershipstatus, loginprovider, mobileno, email, 
                allowreceiveemail, allowreceivesms, idtype, idcard, facebookid, lineid, 
                app24, app24noti, googleid, appleid, app24lastlogindatetime, createdatetime 
            """,
            "from": "etl.view_membership",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"mobileno", "email", "idcard", "facebookid", "lineid", "googleid", "appleid"}
            },
            "object_for_update": {
                "table_name": "membership",
                "field_name": ["membershipid"]
            }
        },

        # table_name : membersale
        "membersale": {
            "select": """
                membershipid, saleid, pairdate
            """,
            "from": "etl.view_membersale",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "membersale",
                "field_name": ["membershipid", "saleid"]
            }
        },

        # table_name : consent
        "consent": {
            "select": """
                consentid, consenttemplatename, requestdate, requestby, requestchannel,
                leadid, consentdate, consentaccept, consentexpirydate, customername,
                customersurname, citizenid, reftablecolumn, refid, saleid
            """,
            "from": "etl.view_consent",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {"customername", "customersurname", "citizenid"}
            },
            "object_for_update": {
                "table_name": "consentinfo",
                "field_name": ["consentid"]
            }
        },

        # table_name : ecommsale
        "ecommsale": {
            "select": """
                appid, order_id, saledate, saleid, utm_source, utm_medium, utm_campaign, channel
            """,
            "from": "etl.view_ecommsale",
            "where": f"""
                -------------------------
                changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                AND changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                AND NVL(uploadstatus,'X') != 'S'
            """,
            "field_action": {
                "field_encrypt": {}
            },
            "object_for_update": {
                "table_name": "ecommsale",
                "field_name": ["appid"]
            }
        },

###################################### FULL DUMP TABLE ######################################
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