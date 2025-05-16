import pandas as pd
from typing import Generic, TypeVar, Type, Dict, List, Any
from pydantic import BaseModel, ValidationError

from splash.utils.dict_utils import list_to_dict
from splash.utils.logger import setup_logger

logger = setup_logger(__name__)

T = TypeVar("T", bound=BaseModel)


class BaseTransformer(Generic[T]):
    """
    A generic base transformer class to transform raw JSON-like dictionaries 
    into validated and structured data using a provided Pydantic model.
    
    Args:
        data (List[Dict[str, Any]]): Raw list of dictionaries to transform.
        model (Type[T]): A Pydantic model type used for validation and transformation.

    Attributes:
        raw_data: Raw input data to be transformed.
        model: The Pydantic model used to structure the data.
    """
    raw_data: List[Dict[str, Any]]
    model: Type[T]
    
    def __init__(self, data: List[Dict[str, Any]], model: Type[T]) -> None:
        self.raw_data = data
        self.model = model
    
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]: # default: no transformation
        """
        Optional transformation logic to be overridden in child classes.

        Args:
            item (Dict[str, Any]): A single raw record.

        Returns:
            Dict[str, Any]: Transformed record. Defaults to identity function.
        """
        return item
    
    def apply_transformation(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Applies the `transform()` function and model validation to a list of records.

        Args:
            data (List[Dict[str, Any]]): Raw input data.

        Returns:
            List[Dict[str, Any]]: List of validated and transformed records.
        """
        transformed: List[Dict[str, Any]] = []
        for item in data:
            try:
                item_transformed = self.transform(item.copy())
                obj = self.model(**item_transformed)  # model validation using pydantic
                transformed.append(obj.model_dump())
            
            except ValidationError as e:
                logger.warning(f"ID: {item.get('id')} | [{type(self).__name__}] Invalid Record: {e}")
            
            except Exception as e:
                logger.warning(f"Source: {item} | [{type(self).__name__}] Error during transformation: {e}")
        
        return transformed
    
    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Post-processes a DataFrame to deduplicate on 'id' column if present.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Deduplicated DataFrame.
        """
        if 'id' not in df.columns:
            logger.warning(f"[{type(self).__name__}] 'id' column not found in DataFrame. Skip processing.")
            return df
            
        if df.id.is_unique:
            return df
        
        else: # Id column has duplicates
            duplicated_ids = df.id.loc[df.id.duplicated(keep=False)].values
            logger.warning(f"[{type(self).__name__}] Duplicated IDs found: {duplicated_ids.tolist()}")
            return df.drop_duplicates(subset=['id'], keep='last', ignore_index=True)  # Deduplication of Rows on Id column
    
    def transform_to_df(self) -> pd.DataFrame:
        """
        Transforms raw data into a clean, deduplicated DataFrame.

        Returns:
            pd.DataFrame: The final cleaned and validated DataFrame.
        """
        transformed_list = self.apply_transformation(self.raw_data)
        logger.info(f"[{type(self).__name__}] Transformed {len(transformed_list):,} valid records.")
        
        transformed_dict = list_to_dict(transformed_list)
        transformed_df = pd.DataFrame(transformed_dict)
        
        if not transformed_df.empty:
            transformed_df.drop_duplicates(ignore_index=True, inplace=True)
            transformed_df = self.process_df(transformed_df)
            logger.info(f"[{type(self).__name__}] Deduplicated and processed into {transformed_df.shape[0]:,} records.")
        
        return transformed_df
