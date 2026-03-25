from qbo_pipeline.qa.small_talk import try_small_talk_reply


def test_hello_returns_intro():
    r = try_small_talk_reply("hello")
    assert r is not None
    assert "QuickBooks" in r


def test_greeting_with_punctuation():
    assert try_small_talk_reply("Hey there!") is not None


def test_data_question_not_small_talk():
    assert try_small_talk_reply("hello how many unpaid invoices") is None
    assert try_small_talk_reply("total payments this month") is None
    assert try_small_talk_reply("anyone owe us money") is None


def test_thanks():
    r = try_small_talk_reply("thank you")
    assert r is not None
    assert "welcome" in r.lower()
