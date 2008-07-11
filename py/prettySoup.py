import re

def pretty(html):
    """remove distracting whitespaces and newline characters; Soup bug?"""
    pat = re.compile('(^[\s]+)|([\s]+$)', re.MULTILINE)
    html = re.sub(pat, '', html)       # remove leading and trailing whitespaces
    html = re.sub('\n|(&nbsp;)', ' ', html)     # convert newlines to spaces
    html = re.sub('[\s]+<', '<', html) # remove whitespaces before opening tags
    html = re.sub('>[\s]+', '>', html) # remove whitespaces after closing tags
    html = re.sub('<b>|<\/b>|<br\s\/>;', '', html)
    html = re.sub('<b>|<\/b>|<br\s\/>;', '', html)
    #zameni = [('&amp;', '&'), ('&quot;', '')]
    #for i, j in zameni:
    #    html = html.replace(i, j)
    return html