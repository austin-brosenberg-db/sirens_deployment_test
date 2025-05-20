from databricks.sirens.udfs.free_email_provider import uses_free_email_provider, uses_free_email_provider_udf
from tests.utils import *

def test_uses_free_email_provider():
    # emails
    assert True is uses_free_email_provider("jason@aol.com")
    assert True is uses_free_email_provider("jason@att.net")
    assert True is uses_free_email_provider("jason@comcast.net")
    assert True is uses_free_email_provider("jason@facebook.com")
    assert True is uses_free_email_provider("jason@gmail.com")
    assert True is uses_free_email_provider("jason@gmx.com")
    assert True is uses_free_email_provider("jason@googlemail.com")
    assert True is uses_free_email_provider("jason@hotmail.co.uk")
    assert True is uses_free_email_provider("jason@hotmail.com")
    assert True is uses_free_email_provider("jason@live.com")
    assert True is uses_free_email_provider("jason@mac.com")
    assert True is uses_free_email_provider("jason@mail.com")
    assert True is uses_free_email_provider("jason@me.com")
    assert True is uses_free_email_provider("jason@msn.com")
    assert True is uses_free_email_provider("jason@sbcglobal.net")
    assert True is uses_free_email_provider("jason@verizon.net")
    assert True is uses_free_email_provider("jason@yahoo.co.uk")
    assert True is uses_free_email_provider("jason@yahoo.com")

    # domains
    assert True is uses_free_email_provider("aol.com")
    assert True is uses_free_email_provider("att.net")
    assert True is uses_free_email_provider("comcast.net")
    assert True is uses_free_email_provider("facebook.com")
    assert True is uses_free_email_provider("gmail.com")
    assert True is uses_free_email_provider("gmx.com")
    assert True is uses_free_email_provider("googlemail.com")
    assert True is uses_free_email_provider("hotmail.co.uk")
    assert True is uses_free_email_provider("hotmail.com")
    assert True is uses_free_email_provider("live.com")
    assert True is uses_free_email_provider("mac.com")
    assert True is uses_free_email_provider("mail.com")
    assert True is uses_free_email_provider("me.com")
    assert True is uses_free_email_provider("msn.com")
    assert True is uses_free_email_provider("sbcglobal.net")
    assert True is uses_free_email_provider("verizon.net")
    assert True is uses_free_email_provider("yahoo.co.uk")
    assert True is uses_free_email_provider("yahoo.com")

    # emails
    assert False is uses_free_email_provider('jason@databricks.com')
    assert False is uses_free_email_provider('jason@walmart.com')
    assert False is uses_free_email_provider('jason@microsoft.com')
    assert False is uses_free_email_provider('jason@amazon.com')
    assert False is uses_free_email_provider('jason@apple.com')
    assert False is uses_free_email_provider("jason@google.com")

    # domains
    assert False is uses_free_email_provider('databricks.com')
    assert False is uses_free_email_provider('walmart.com')
    assert False is uses_free_email_provider('microsoft.com')
    assert False is uses_free_email_provider('amazon.com')
    assert False is uses_free_email_provider('apple.com')
    assert False is uses_free_email_provider("google.com")

    assert False is uses_free_email_provider(None)

@pytest.mark.usefixtures("spark_session")
def free_uses_free_email_provider_udf(spark_session):
    """Test extract username with email address form from userIdentity.principalId"""

    df = spark_session.createDataFrame(Row(rec={"email": 'jason@gmail.com'}), schema=['email'])
    assert True is df.withColumn('flag', uses_free_email_provider_udf('email')).head().flag

    df = spark_session.createDataFrame(Row(rec={"email": 'jason@databricks.com'}), schema=['email'])
    assert False is df.withColumn('flag', uses_free_email_provider_udf('email')).head().flag
    
