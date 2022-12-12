def get_home_away(x):
    try:
        if "vs." in x:
            return "Home"
        elif "@" in x:
            return "Away"
    except:
        return None