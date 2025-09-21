import services.utils as utils
import tiktoken
import re
import logging
import spacy
import copy
from services.constants import DEFAULT_SENTENCES_REGROUP_RATIO

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
                      chunk_overlap_lines: int = 0,
                      chunking_strategy: str = "aggregate_lines") -> list[str]:
    '''
        Split the lines into chunks.
        Args:
            lines: The lines of conetent to be separated into chunks
            max_token_count: The max token count for each chunk
            model_name: The model name for the tokenizer
            tag_content: The tag that will be added to each chunk. Usually it stands for the topic/category of this lines of content.
            tag_content_allowed_token: The max token count allowed for the tag_content.
            chunk_overlap_lines: How many lines from previous chunk is allowed to overlap into the next chunk. For better context preservation.
            chunking_strategy: The strategy to use for chunking (Options: "aggregate_lines", "one_line_per_chunk")
        Returns:
            A list of text, and each stands for a chunk

        Limitation:
            Currently, it can't handle if a tag_content itself exceeds the max_token_count. But in most case, we shouldn't have this problem.
    '''
    # Check if the chunking strategy is valid
    if chunking_strategy not in ["aggregate_lines", "one_line_per_chunk"]:
        raise ValueError(f"Invalid chunking strategy: {chunking_strategy}")

    # Deep copy the lines to avoid modifying the original list
    lines = copy.deepcopy(lines)

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

    # In case any "tag_content + line" is too long, try to split the line into multiple sentences.
    # This section of code carries an assumption that the tag_content is usually not long. 
    lines_processed = []
    for line in lines:
        if utils.get_token_count(tag_content + line, model_name) > max_token_count: 
            '''
                NOTE: It assume "a sentence" + "tag_count" can very hardly exceed the max_token_count. 
                This simple method will help preserve the sentence boundaries and context of the original text.
            '''           

            # Split the line into sentences and add them back.
            # You might say splitting into sentences will break the context. But the chunk aggregation later shall group them back.
            sentences = split_text_into_sentences(line)

            # Treat the sentences as lines to group them back but make sure less than 70% of "max_token_count"
            # Try to maintain the context as much as possible.
            grouped_sentences = _chunk_aggregate_lines(lines =sentences, 
                                                    tag_content = tag_content, 
                                                    chunk_overlap_lines = 0, 
                                                    max_token_count = max_token_count * DEFAULT_SENTENCES_REGROUP_RATIO, 
                                                    model_name = model_name)

            lines_processed.extend(grouped_sentences)
        else:
            lines_processed.append(line)

    # :::::: Start Chunk Splitting :::::: #

    if chunking_strategy == "one_line_per_chunk":
        return _chunk_one_line_per_chunk(lines_processed, tag_content, chunk_overlap_lines)
    elif chunking_strategy == "aggregate_lines":
        return _chunk_aggregate_lines(lines_processed, tag_content, chunk_overlap_lines, max_token_count, model_name)


def _chunk_one_line_per_chunk(lines_processed, tag_content, chunk_overlap_lines) -> list[str]:
    '''
        Please treat this as a internal helper function!

        Split the lines into chunks, each line is a chunk.
        Args:
            lines_processed: The lines of conetent to be separated into chunks
            tag_content: The tag that will be added to each chunk. Usually it stands for the topic/category of this lines of content.
            chunk_overlap_lines: How many lines from previous chunk is allowed to overlap into the next chunk. For better context preservation.
    '''
    chunks = []
    for i, line in enumerate(lines_processed):
        # base chunk with tag + line
        chunk_lines = [tag_content, line] if tag_content else [line]

        # add overlap from previous lines (not including current line)
        if chunk_overlap_lines > 0 and i > 0:
            overlap_start = max(0, i - chunk_overlap_lines)
            overlap = lines_processed[overlap_start:i]
            chunk_lines = [tag_content] + overlap + [line] if tag_content else overlap + [line]

        chunks.append("\n".join(chunk_lines))
    return chunks


def _chunk_aggregate_lines(lines, tag_content, chunk_overlap_lines, max_token_count, model_name) -> list[str]:
    '''
        Please treat this as a internal helper function!

        Aggregate the lines into chunks.
        Args:
            lines_processed: The lines of conetent to be separated into chunks
            tag_content: The tag that will be added to each chunk. Usually it stands for the topic/category of this lines of content.
            chunk_overlap_lines: How many lines from previous chunk is allowed to overlap into the next chunk. For better context preservation.
            max_token_count: The max token count for each chunk
            model_name: The model name for the tokenizer
    '''
    chunks = []
    curr_chunk = [tag_content]
    i = 0

    while i < len(lines):
        curr_line = lines[i]
        curr_chunk.append(curr_line)
        curr_chunk_str = "\n".join(curr_chunk)

        if utils.get_token_count(curr_chunk_str, model_name) > max_token_count:
            curr_chunk.pop()
            if len(curr_chunk) > 1:
                chunks.append("\n".join(curr_chunk))

            # prepare overlap
            content_part = curr_chunk[1:]  # exclude tag
            eff_overlap = min(chunk_overlap_lines, len(content_part))
            overlap = content_part[-eff_overlap:] if eff_overlap > 0 else []
            curr_chunk = [tag_content] + overlap
            continue
        else:
            i += 1

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