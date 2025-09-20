import services.utils as utils
import tiktoken
import re
import logging
import spacy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# load spaCy English model for sentence splitting (can be "en_core_web_sm" or larger if needed)
spacy_nlp = spacy.load("en_core_web_sm")

def split_text_into_sentences(text: str) -> list[str]:
    '''
        Split a text into sentences leverage spaCy NLP model.
    '''
    doc = spacy_nlp(text)
    sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]

    return sentences


def split_into_chunks(lines: list[str], 
                      max_token_count: int, 
                      model_name: str,
                      tag_content: str = None,
                      tag_content_allowed_token: int = 30,
                      chunk_overlap_lines: int = 0) -> list[str]:
    '''
        Split the lines into chunks.
        Args:
            lines: The lines of conetent to be separated into chunks
            max_token_count: The max token count for each chunk
            model_name: The model name for the tokenizer
            tag_content: The tag that will be added to each chunk. Usually it stands for the topic/category of this lines of content.
            tag_content_allowed_token: The max token count allowed for the tag_content.
            chunk_overlap_lines: How many lines from previous chunk is allowed to overlap into the next chunk. For better context preservation.
        Returns:
            A list of text, and each stands for a chunk

        Limitation:
            Currently, it can't handle if a tag_content itself exceeds the max_token_count. But in most case, we shouldn't have this problem.
    '''
    lines = lines.copy()

    # Regularize the tag_content
    tag_content = tag_content if tag_content else ""

    # :::::: Early Termination Cases :::::: #

    # Try if the full text is small enough to be a chunk or not
    full_text = "\n".join([tag_content] + lines)
    if utils.get_token_count(full_text, model_name) <= max_token_count:
        return [full_text]

    # :::::: Preprocessing :::::: #

    # If the tag_content is too long, making it a line rather than a tag_content
    if tag_content.strip() != "" and utils.get_token_count(tag_content, model_name) > tag_content_allowed_token:
        lines.insert(0, tag_content)
        tag_content = ""

    # In case any "tag_content + line" is too long, try to split the line into multiple lines
    # This section of code carries an assumption that the tag_content is usually not long. 
    lines_processed = []
    for line in lines:
        if utils.get_token_count(tag_content + line, model_name) > max_token_count: 
            '''
                NOTE: It assume "a sentence" + "tag_count" can very hardly exceed the max_token_count. 
                This simple method will help preserve the sentence boundaries and context of the original text.
            '''           

            # Split the line into sentences and add them back.
            sentences = split_text_into_sentences(line)
            lines_processed.extend(sentences)
        else:
            lines_processed.append(line)

    # :::::: Start Chunk Splitting :::::: #

    # Recording the final output
    chunks = []

    # Split the lines into different chunks with tag content
    curr_chunk = [tag_content]
    i = 0

    while i < len(lines_processed):
        # Current line
        curr_line = lines_processed[i]

        # Try to create this chunk
        curr_chunk.append(curr_line)
        curr_chunk_str = "\n".join(curr_chunk)

        # If the chunk is too large, remove the last line added, and commit the chunk
        if utils.get_token_count(curr_chunk_str, model_name) > max_token_count:
            # remove the oversized line we just added
            curr_chunk.pop()

            # commit only if there is content beyond the tag_content
            if len(curr_chunk) > 1:
                chunks.append("\n".join(curr_chunk))

            '''
              NOTE: If len(curr_chunk) == 1, it means only tag_content is present. This case shouldn't occur because preprocessing 
              ensures no single line exceeds max_token_count. So, we can safely proceed.
            '''

            # -------- prepare next chunk with safe overlap -------- #
            
            # overlap should come only from the content part (exclude tag_content)
            content_start_idx = 1
            content_part = curr_chunk[content_start_idx:]

            # cap overlap to available lines
            eff_overlap = min(chunk_overlap_lines, len(content_part))
            overlap = content_part[-eff_overlap:] if eff_overlap > 0 else []

            # shrink overlap until adding the current line will fit next time
            base = [tag_content] + overlap
            while overlap and utils.get_token_count("\n".join(base + [curr_line]), model_name) > max_token_count:
                overlap.pop(0)                  # drop the earliest overlap line
                base = [tag_content] + overlap  # reset the base

            # reset current chunk to (tag + adjusted overlap); retry same line
            curr_chunk = base
            continue  # DO NOT advance i; we will re-add curr_line to the new curr_chunk
        else:
            # fits: advance to next line
            i += 1

    # flush the tail if there is any content beyond the tag_content
    if len(curr_chunk) > 1:
        chunks.append("\n".join(curr_chunk))

    return chunks
    

def split_text_by_token_limit_BRUTE(text: str, model_name: str, max_tokens: int, overlap: int = 0):
    """
    A Brute Force Splitter to split `text` into a list of chunks, each having <= max_tokens tokens
    according to the tokenizer for `model_name`.

    NOTE: This is a brute force splitter, it doesn't understand the sentecne boundaries. So, it can cut off the sentences.

    Args:
    text: the long string
    model_name: embedding model name, e.g. "text-embedding-3-large"
    max_tokens: max tokens per chunk
    overlap: overlap between chunks to preserve context

    Returns:
    List of chunk strings
    """
    logger.info(f"Brute force splitter got: {text}")

    # Get encoding for the model
    enc = tiktoken.encoding_for_model(model_name)

    # Encode full text into tokens
    token_ids = enc.encode(text)
    n = len(token_ids)

    if n <= max_tokens:
        return [text]

    out = []
    start = 0

    while start < n:
        end = min(start + max_tokens, n)
        # Decode back to text the slice
        chunk_tokens = token_ids[start:end]
        chunk_text = enc.decode(chunk_tokens)
        out.append(chunk_text)

        if end == n:
            break

        # Move start forward, minus overlap
        start = end - overlap

    return out