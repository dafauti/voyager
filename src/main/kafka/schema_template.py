student_Schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        }
    },
    "required": [
        "name"
    ]
}

xml_template_no_employee = '''<?xml version="1.0" encoding="utf-8"?>
<xs:schema attributeFormDefault="unqualified" elementFormDefault="qualified" xmlns:xs="http://www.w3.org/2001/XMLSchema">
<xs:element name="OFX">
<xs:complexType>
<xs:sequence>
<xs:element name="SIGNONMSGSRSV1">
<xs:complexType>
<xs:sequence>
<xs:element name="SONRS">
<xs:complexType>
<xs:sequence>
<xs:element name="STATUS">
<xs:complexType>
<xs:sequence>
<xs:element name="CODE" type="xs:unsignedByte" />
<xs:element name="SEVERITY" type="xs:string" />
</xs:sequence>
</xs:complexType>
</xs:element>
<xs:element name="DTSERVER" type="xs:unsignedLong" />
<xs:element name="LANGUAGE" type="xs:string" />
</xs:sequence>
</xs:complexType>
</xs:element>
</xs:sequence>
</xs:complexType>
</xs:element>
<xs:element name="TSVERMSGSRSV1">
<xs:complexType>
<xs:sequence>
<xs:element name="TSVTWNSELECTTRNRS">
<xs:complexType>
<xs:sequence>
<xs:element name="TRNUID" type="xs:string" />
<xs:element name="STATUS">
<xs:complexType>
<xs:sequence>
<xs:element name="CODE" type="xs:unsignedShort" />
<xs:element name="SEVERITY" type="xs:string" />
<xs:element name="MESSAGE" type="xs:string" />
<xs:element name="MESSAGE1" type="xs:string" minOccurs="0" maxOccurs="1"/>
</xs:sequence>
</xs:complexType>
</xs:element>
</xs:sequence>
</xs:complexType>
</xs:element>
</xs:sequence>
</xs:complexType>
</xs:element>
</xs:sequence>
</xs:complexType>
</xs:element>
</xs:schema>'''

xml_template = '''<?xml version="1.0" encoding="utf-8"?>
<!-- Created with Liquid Technologies Online Tools 1.0 (https://www.liquid-technologies.com) -->
<xs:schema attributeFormDefault="unqualified" elementFormDefault="qualified" xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="OFX">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="SIGNONMSGSRSV1">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="SONRS">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="STATUS">
                      <xs:complexType>
                        <xs:sequence>
                          <xs:element name="CODE" type="xs:string" />
                          <xs:element name="SEVERITY" type="xs:string" />
                        </xs:sequence>
                      </xs:complexType>
                    </xs:element>
                    <xs:element name="DTSERVER" type="xs:unsignedLong"/>
                    <xs:element name="LANGUAGE" type="xs:string"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="TSVERMSGSRSV1">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="TSVTWNSELECTTRNRS">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="TRNUID" type="xs:string" />
                    <xs:element name="STATUS">
                      <xs:complexType>
                        <xs:sequence>
                          <xs:element name="CODE" type="xs:string" />
                          <xs:element name="SEVERITY" type="xs:string" />
                          <xs:element name="MESSAGE" type="xs:string" minOccurs="0" maxOccurs="1"/>
                        </xs:sequence>
                      </xs:complexType>
                    </xs:element>
                    <xs:element name="MASTERSRVRTID" type="xs:unsignedLong" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="TRNPURPOSE" minOccurs="0" maxOccurs="1">
                      <xs:complexType>
                        <xs:sequence>
                          <xs:element name="CODE" type="xs:string" />
                          <xs:element name="MESSAGE" type="xs:string" />
                        </xs:sequence>
                      </xs:complexType>
                    </xs:element>
                    <xs:element name="TSVTWNSELECTRS" minOccurs="0" maxOccurs="1">
                      <xs:complexType>
                        <xs:sequence>
                          <xs:element maxOccurs="unbounded" name="TSVRESPONSE_V100">
                            <xs:complexType>
                              <xs:sequence>
                                <xs:element name="DTTRANSACTION" type="xs:unsignedLong" />
                                <xs:element name="TSVEMPLOYER_V100">
                                  <xs:complexType>
                                    <xs:sequence>
                                      <xs:element name="EMPLOYERCODE" type="xs:unsignedInt" />
                                      <xs:element name="NAME1" type="xs:string" />
                                      <xs:element name="ADDR1" type="xs:string" />
                                      <xs:element name="ADDR2" />
                                      <xs:element name="CITY" type="xs:string" />
                                      <xs:element name="STATE" type="xs:string" />
                                      <xs:element name="POSTALCODE" type="xs:string" />
                                      <xs:element minOccurs="0" name="DISCLAIMER" type="xs:string" />
                                      <xs:element minOccurs="0" name="ITEMIZEDDISCLAIMERS">
                                        <xs:complexType>
                                          <xs:sequence>
                                            <xs:element name="DISCLAIMERTYPE" type="xs:string" />
                                            <xs:element name="DISCLAIMERTEXT" type="xs:string" />
                                          </xs:sequence>
                                        </xs:complexType>
                                      </xs:element>
                                      <xs:element name="EMPLOYERDISPLAY">
                                        <xs:complexType>
                                          <xs:sequence>
                                            <xs:element name="EMPLOYERDISPLAYINFO">
                                              <xs:complexType>
                                                <xs:sequence>
                                                  <xs:element name="LANGUAGE" type="xs:string" />
                                                  <xs:element name="LOGOFILENAME" type="xs:string" />
                                                  <xs:element name="VOICEFILENAME" />
                                                </xs:sequence>
                                              </xs:complexType>
                                            </xs:element>
                                          </xs:sequence>
                                        </xs:complexType>
                                      </xs:element>
                                    </xs:sequence>
                                  </xs:complexType>
                                </xs:element>
                                <xs:element name="TSVEMPLOYEE_V100">
                                  <xs:complexType>
                                    <xs:sequence>
                                      <xs:element name="SSN" type="xs:unsignedInt" />
                                      <xs:element name="ALTERNATEID" />
                                      <xs:element name="FIRSTNAME" type="xs:string" />
                                      <xs:element name="MIDDLENAME" />
                                      <xs:element name="LASTNAME" type="xs:string" />
                                      <xs:element name="POSITION-TITLE" type="xs:string" />
                                      <xs:element name="DIVISIONCODE" />
                                      <xs:element name="EMPLOYEESTATUS">
                                        <xs:complexType>
                                          <xs:sequence>
                                            <xs:element name="CODE" type="xs:unsignedByte" />
                                            <xs:element name="MESSAGE" type="xs:string" />
                                          </xs:sequence>
                                        </xs:complexType>
                                      </xs:element>
                                      <xs:element name="DTINFO" type="xs:unsignedLong" />
                                      <xs:element name="DTMOSTRECENTHIRE" type="xs:unsignedLong" />
                                      <xs:element name="DTORIGINALHIRE" type="xs:unsignedLong" />
                                      <xs:element name="TOTALLENGTHOFSVC" type="xs:unsignedByte" />
                                      <xs:element name="DTENDEMPLOYMENT" type="xs:string" />
                                    </xs:sequence>
                                  </xs:complexType>
                                </xs:element>
                                <xs:element name="TSVOFFWORK" />
                                <xs:element name="COMPLETENESS" type="xs:string" />
                                <xs:element name="SRVRTID" type="xs:unsignedLong" />
                                <xs:element name="DEMOTRN">
                                  <xs:complexType>
                                    <xs:sequence>
                                      <xs:element name="DEMOTYPE" />
                                    </xs:sequence>
                                  </xs:complexType>
                                </xs:element>
                              </xs:sequence>
                            </xs:complexType>
                          </xs:element>
                        </xs:sequence>
                      </xs:complexType>
                    </xs:element>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''

xml_schema_return='''<?xml version="1.0" encoding="utf-8"?>
<xs:schema attributeFormDefault="unqualified" elementFormDefault="qualified" xmlns:xs="http://www.w3.org/2001/XMLSchema">
<xs:element name="return">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ClientData">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="AccountId" type="xs:unsignedShort" />
              <xs:element name="OrganizationName" type="xs:string" />
              <xs:element name="CaseReferenceId" type="xs:string" />
              <xs:element name="ContactEmail" type="xs:string" />
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''