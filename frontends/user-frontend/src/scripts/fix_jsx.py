
import os

path = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\frontends\user-frontend\src\pages\BrokerConnection.tsx"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Fix malformed tags
content = content.replace("< div", "<div")
content = content.replace("</div >", "</div>")
content = content.replace("</motion.div >", "</motion.div>")
content = content.replace("< /div >", "</div>")

# Remove extra closing div if present around line 339 (contextual)
# Context: 
#   </div>
# </div>
# </div>
# 
# {/* Capital Section */}

bad_block = """                                </div>
                                    
                                    {/* Capital Section */}"""

# We want to remove one </div> from here if it's the extra one.
# But let's look at the structure.
# If I simply fix the tags, the code might still have an extra div.

# Let's write the fixed content first.
with open(path, "w", encoding="utf-8") as f:
    f.write(content)

print("Fixed tags.")
